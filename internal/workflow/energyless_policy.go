// Decisioni (aggiornato dopo il ricevimento del 02/10 con Cardellini/Cecilia/Russo Russo):
//  1. Decomposizione NON verrà implementata — sostituita dalla selezione tra
//     funzione base e varianti (Function.DefaultFunction/SpeedUp/Utility).
//     Implementata in selectBest(): per ogni candidato si valuta sia la
//     funzione base sia ogni sua variante valida, e si sceglie la coppia
//     (candidato, funzione-o-variante) con l'obiettivo minimo. La variante
//     scelta viene poi eseguita davvero — vedi punto 16 più sotto.
//  2. Stima calcolata in Go locale, nessun optimizer esterno via HTTP.
//     3+8) Batch di dimensione 1: nessun ranking tra workflow/richieste diverse,
//     Evaluate() ragiona solo sulla richiesta corrente.
//  4. Normalizzazione dell'obiettivo (Eq. 22 del paper) sul massimo osservato
//     tra le coppie (candidato, funzione-o-variante) valutate per il singolo
//     task durante la costruzione del piano (buildPlan/selectBest), non
//     condivisa tra più task.
//  5. Dati di potenza per nodo: NON serve una config nuova — esistono già
//     consumption.processing.power / consumption.energy.tx / .rx per nodo
//     (internal/config/keys.go, lette in node.Resources.Init()) e vengono
//     già propagate ai vicini via heartbeat UDP dentro
//     registration.StatusInformation. Per il target CLOUD: deciso 02/10 che
//     un nodo Edge NON interroga il Cloud per le risorse residue — si
//     assume che il Cloud abbia risorse (memoria/CPU) infinite, quindi il
//     vincolo di capacità è saltato per quel candidato (vedi
//     estimateOnCandidate). La co2Intensity del cloud, invece, viene
//     interrogata live (fetchCloudStatus(), stesso pattern HTTP GET
//     "<APIUrl>/status" di fetchNodeStatus() in
//     internal/scheduling/co2_qos_policy.go — confermato da Cecilia il
//     16/10 come meccanismo corretto), con fallback sull'ultimo valore
//     noto del cloud (non più sulla CO2Intensity del nodo locale, decisione
//     del 16/10 dopo che Cecilia ha confermato che il
//     fallback locale non era una scelta di design validata) — vedi
//     gatherCandidates().
//     [NUOVO dal pull del 16/10, commit "fix avg cloud zone retrieval
//     metrics + add idle power" di Cecilia] È stata aggiunta una potenza
//     IDLE per nodo/area (consumption.idle.power, config.IDLE_POWER_CONSUMPTION,
//     node.Resources.IdlePower(), AreaInfo.IdlePowerConsumption,
//     AreaStat.IdlePowerConsumption, registration.StatusInformation.IdlePowerConsumption
//     — propagata via heartbeat UDP e via /status esattamente come
//     ProcessingPowerConsumption), con valori di esempio reali in
//     examples/carbon/ (800W→300W idle cloud, 100W→40W idle edge).
//     [DECISIONE RIVISTA il 17/10] La potenza idle è ora
//     REINTEGRATA nell'Eq. 7 (vedi elCandidate.idlePowerW e energyJoules) —
//     la decisione del 02/10 di ignorarla "in linea col paper originale"
//     viene quindi superata da questa modifica.
//     [CONFERMATO nel ricevimento del 22/10] Reintegrazione e forma
//     validate dal gruppo: confermata l'incongruenza dimensionale rilevata
//     (ε_i definita come potenza ma sommata direttamente senza
//     conversione temporale nell'Eq. 7 del paper originale). Il consumo
//     energetico va calcolato come Potenza × Tempo di esecuzione = Joule,
//     quindi idlePowerW * execTimeSec — applicato in energyJoules().
//     [SUPERATO il 20/11, punto 22] Dopo il ricevimento del 18/11 l'idle non
//     entra più nella funzione obiettivo (resta in idleEnergyJoules() per i
//     conti a posteriori).
//     [NUOVO dal pull del 16/10] Fix di Cecilia alla query Prometheus per
//     le metriche medie del cloud (internal/metrics/retriever.go,
//     cloudRegionAverageByFunctionQuery/cloudRegionColdStartProbabilityQuery):
//     la vecchia query divideva _sum/_count senza aggregazione per nodo,
//     risultato potenzialmente sbagliato con più nodi fisici nella stessa
//     area cloud; la nuova fa sum by (function, node) prima di dividere, poi
//     avg by (function). Questo alimenta rm.AvgRemoteExecutionTime, già letto
//     da estimateExecTime() in questa bozza per il candidato CLOUD — nessuna
//     modifica di codice necessaria qui, beneficio automatico in termini di
//     accuratezza della stima del tempo di esecuzione sul cloud.
//  6. Piano di scheduling in CACHE (non più rivalutazione ad ogni round):
//     deciso 02/10, come nel paper originale e come già fanno ILP/HEFTless.
//     Evaluate() ora calcola il placement per l'intera catena di task non
//     ancora eseguiti UNA SOLA VOLTA (buildPlan), lo cachea per richiesta
//     (cacheSolution/getCachedPlan, chiave r.Id). Verificato sul codice reale
//     di ILP/HEFTless (remote_offloading_policy.go): la cache NON scade per
//     tempo reale (come assunto in una versione precedente di questo file),
//     ma per numero di LETTURE residue (ttl int, decrementato a ogni round
//     che riusa il piano) — replicato identicamente qui. Nei round
//     successivi il piano cachato viene tradotto in decisione senza
//     ricalcolare (computeDecisionFromPlacement). La rivalutazione dinamica
//     del piano resta rimandata a una fase successiva (come per HEFTless,
//     che non supporta re-scheduling a metà esecuzione).
//  7. Soglia di concorrenza ρ_i (Eq. 21 del paper): chiarito 02/10 che
//     corrisponde al parametro "available CPU" già presente in Serverledge
//     — il controllo di capacità CPU già esistente in estimateOnCandidate
//     è già la traduzione corretta, non serve un vincolo separato.
//  9. Offloading a un solo hop: nessuna logica aggiuntiva necessaria — è
//     già garantito dal flag r.CanDoOffloading, controllato qui esattamente
//     come in HEFTlessPolicy/IlpOffloadingPolicy.
//  10. Strutture di workflow supportate: deciso 02/10 di restare sulle
//     catene sequenziali (chain) — buildPlan() cammina il DAG seguendo
//     FunctionTask.NextTask e si ferma al primo nodo non-FunctionTask
//     (es. ChoiceTask non ancora risolto). Il supporto a FanOut/FanIn è
//     rimandato a quando sarà stabile il lavoro dell'altro tesista.
//  11. Contenuto dell'offloading plan, chiarito nel ricevimento del 02/10:
//     il piano inviato al nodo remoto scelto NON si limita ai task
//     "pronti ora" o consecutivi — include TUTTI i task del placement
//     cachato assegnati a quel nodo per l'intera richiesta (visione
//     totale). Il nodo remoto, ricevuto il piano, esegue i task i cui
//     dati di input sono già disponibili e, se incontra un task ancora in
//     attesa di dati prodotti altrove, risponde subito ignorandolo
//     temporaneamente; il nodo sorgente prosegue con le altre fasi del
//     workflow. Implementato in computeDecisionFromPlacement(), dal 21/11
//     computeDecisionsFromPlacement() (punto 24): toExecute
//     è costruito iterando sull'intero placement cachato (filtrato per
//     chosenRemote), non più solo su p.ReadyToExecute.
//  12. [BUG RISOLTO 17/10, confrontando il codice
//     col paper] buildPlan() usciva con un placement VUOTO al primo round:
//     InitProgress() (progress.go) mette in ReadyToExecute, al round 1,
//     solo workflow.Start.Id — uno *StartTask, non un *FunctionTask. Il
//     filtro "non è un FunctionTask → continue" scartava questo task SENZA
//     accodare il suo successore, la coda si vuotava subito, e il
//     placement vuoto veniva cachato con TTL ~9999 — la policy non
//     offloadava mai più per tutta la vita della richiesta. Corretto in
//     buildPlan(): i task non-FunctionTask che implementano comunque
//     UnaryTask (StartTask, PassTask, FailureTask: hanno un GetNext()
//     deterministico) vengono ora attraversati senza fare placement; la
//     catena si interrompe davvero solo per un ConditionalTask (ChoiceTask,
//     il cui next dipende da dati a runtime) o per un task senza
//     successore (EndTask).
//  13. [BUG RISOLTO 17/10, dalla voce "Da sistemare"
//     — scadenza non cumulativa] residualDeadline viene calcolata UNA VOLTA
//     sola in Evaluate() (QoS.MaxRespT meno il tempo già trascorso
//     dall'arrivo della richiesta) e prima di questa correzione veniva
//     passata INVARIATA a selectBest()/estimateOnCandidate() per OGNI task
//     della catena costruita da buildPlan() — cioè ogni task, anche il
//     decimo di una catena lunga, veniva confrontato con l'intera finestra
//     residua come se fosse l'unico task rimasto da eseguire (Eq. 5 del
//     paper applicata senza la somma cumulativa di Eq. 4). Risultato: un
//     piano con più task in sequenza, ciascuno singolarmente entro
//     deadline, poteva comunque sforare la deadline complessiva della
//     richiesta nel suo insieme, perché buildPlan non teneva conto del
//     tempo già "impegnato" dai task precedenti nella stessa catena.
//     [21/11, punto 24] elapsedChain è sostituito dall'inizio stimato di
//     ciascun task (startAt), che vale anche per i rami paralleli.
//     Corretto in buildPlan(): un accumulatore elapsedChain somma il
//     completionTime (Eq. 2-4) di ogni task via via posizionato nella
//     catena corrente; il task successivo riceve come budget
//     "residualDeadline - elapsedChain" invece della residualDeadline
//     originale invariata. Se il budget residuo scende a zero o sotto
//     zero, selectBest() non trova più candidati fattibili (stesso
//     comportamento di un vincolo di deadline violato) e il task resta
//     fuori dal placement, come già accadeva per qualunque altro task
//     infattibile — nessuna modifica a selectBest()/estimateOnCandidate(),
//     solo al valore che buildPlan() passa loro.
//  14. [AGGIORNATO 17/10, voce "Da sistemare" — costo/CO2 incompleti
//     rispetto a Eq. 10/14/15] estimateOnCandidate() mancava di quattro
//     termini del modello EnergyLess, rendendo le stime sui candidati
//     remoti sistematicamente più ottimistiche di quanto previsto dal
//     paper:
//     - Cold start (λ_i^f, Eq. 2): ora incluso nel completionTime,
//     stimato come tempo medio di init osservato * probabilità di cold
//     start (nuova estimateInitTime(), stesso approccio già usato da
//     ILP/HEFTless in prepareParameters() per lo stesso scopo).
//     - Vincolo di banda (Eq. 20): prima, un candidato per cui la banda
//     verso il data store non era configurata veniva trattato come "a
//     tempo di trasferimento zero" (ottimistico e sbagliato); ora un
//     simile candidato è semplicemente infattibile.
//     - Costo di trasferimento (CT^f, Eq. 14) e di storage (CS^f, Eq. 15):
//     aggiunti al costo totale per i candidati non locali, con due coppie
//     di chiavi di configurazione NUOVE, promosse in
//     internal/config/keys.go (WORKFLOW_OFFLOADING_POLICY_ENERGYLESS_
//     NODE/CLOUD_TO_DATA_STORE_TRANSFER/STORAGE_COST, vedi commento sopra
//     dataStoreTransferCostPerByte/dataStoreStorageCostPerByte, sostituite il
//     20/11 da linkTransferCostPerByte, punto 23), default
//     0.0 — nessun cambio di comportamento finché non vengono impostate
//     esplicitamente.
//     [RISOLTO nel ricevimento del 22/10, non più un limite aperto] la
//     componente "storage" di Eq. 10 (CO^f_data = ξ_j · E^f_data, dove ξ_j è
//     l'intensità di carbonio dello STORAGE, distinta da quella del
//     candidato di calcolo ξ_i) resta approssimata con ξ_i — confermato
//     dal gruppo che questa è l'approssimazione CORRETTA, perché Serverledge
//     farà riferimento a etcd/al modello di storage centralizzato: non c'è
//     una selezione di storage separati da modellare, quindi non serve una
//     CO2Intensity distinta per lo storage.
//     [SUPERATO il 20/11, punto 23] Nel branch dopo il merge i dati non
//     passano più per un data store: banda, energia e costo dei dati sono
//     calcolati sui trasferimenti diretti tra nodi; il costo di storage
//     (CS^f) non c'è più.
//  15. [NUOVO 30/10, deciso nel ricevimento del 22/10] Vincolo di soglia
//     minima di accuratezza sulla selezione delle varianti. L'accuratezza del
//     workflow è il prodotto delle accuratezze dei task: per la funzione del
//     task vale 1.0, per una variante sostitutiva vale Function.Utility
//     (∈[0,1], già validato da ValidateVariantMetadata). La soglia viene da
//     RequestQoS.MinAccuracy (campo NUOVO, da aggiungere a
//     internal/function/request.go), con default defaultMinAccuracy = 0.95
//     se non impostato (<= 0). buildPlan() porta un accumulatore accChain
//     (stesso schema di elapsedChain per la deadline) e passa a selectBest()
//     l'accuratezza minima del singolo task (soglia / accChain): le varianti
//     sotto tale valore sono scartate prima della stima. Il ciclo
//     candidati × (funzione + varianti) di selectBest() esisteva già dal
//     02/10; questa modifica aggiunge solo il vincolo. Limiti noti: (a)
//     selezione greedy per task, il budget di accuratezza non è ottimizzato
//     sull'intera catena. La ripresa remota non ripianifica (la richiesta
//     offloadata ha CanDoOffloading=false), quindi l'accuratezza cumulata
//     decisa all'origine resta valida anche per i task eseguiti altrove.
//  16. [NUOVO 01/11] Esecuzione della variante scelta. Il piano assegna a
//     ogni task una funzione o una variante; la scelta arriva all'esecuzione
//     così:
//     - Evaluate() ricava dal piano le sostituzioni "task -> variante"
//     (variantChoices) e le scrive in Request.Variants, un campo
//     per-richiesta;
//     - FunctionTask.exec() esegue la variante (resolveVariant) impostando
//     la funzione della function.Request creata per quella singola
//     invocazione: i task del workflow, condivisi tra richieste
//     concorrenti, non vengono modificati, e lo strato di scheduling non
//     deve sapere nulla di varianti (non servono executionTarget né un
//     campo PreferredVariant). Quindi in quella Request Fun è la variante:
//     con scheduler.policy co2qosaware, Co2QosAwarePolicy tratta una
//     richiesta con Fun già variante come non ottimizzabile e rispetta la
//     scelta di EnergyLess (parere di Cecilia, 06/11: va bene);
//     - se dei task vengono offloadati, le varianti dei task in ToExecute
//     viaggiano in OffloadingPlan.Variants e il nodo remoto le rimette in
//     Request.Variants (ResumeWorkflow in internal/api/workflow.go).
//     Fallback: una variante non registrata, non variante di quella
//     funzione o con signature diversa non blocca la richiesta, si esegue la
//     funzione base e lo si scrive nel log. Se la richiesta ha
//     CanDoOffloading=false, Evaluate() non pianifica e ogni task esegue la
//     propria funzione. Modifiche fuori da questo file: workflow/request.go
//     (Variants), workflow/offloading_policy.go (OffloadingPlan.Variants),
//     workflow/function_task.go (resolveVariant), api/workflow.go.
//  17. [BUG RISOLTO 11/11, gestione chiarita con Cecilia l'11/11] Task senza
//     coppia (nodo, funzione) entro la deadline residua: buildPlan() faceva
//     "continue" PRIMA di accodare il successore, quindi la visita si fermava
//     e tutti i task successivi restavano fuori dal piano (in locale per
//     sempre, piano in cache con TTL 9999). Ora il successore si accoda
//     sempre. Cecilia: deadline impossibili (un task che da solo richiede più
//     della deadline del workflow) sono un input incoerente, da evitare negli
//     esperimenti (in futuro andrebbero rifiutate alla creazione del
//     workflow); uno sforamento dovuto a condizioni cambiate a runtime è
//     invece realistico: la richiesta si porta a termine ma, se sfora davvero,
//     conta come NON completata (da misurare negli esperimenti confrontando il
//     tempo di risposta con MaxRespT). Il ripiego sulla coppia più veloce
//     (selectFastest), scelto qui l'11/11, è stato tolto il 20/11: vedi punto
//     21.
//  18. [NUOVO 11/11, suggerimento di Cecilia dell'11/11] Tempi senza
//     metriche. Il ripiego di 0.01 s (esecuzione e init) è un valore
//     generico, non adatto quando la funzione dura secondi: un nodo senza
//     misure sembrerebbe velocissimo. Indicazione di Cecilia: fare un tuning
//     preliminare (eseguire prima le funzioni, così Prometheus ha i tempi
//     reali; vedi scripts/tune-energyless.sh) oppure usare default sensati.
//     Ora i due ripieghi sono configurabili nel file YAML della policy
//     (default.exec.time, default.init.time; senza chiavi resta 0.01 s). Per
//     le varianti si usa lo SpeedUp DICHIARATO, che va registrato uguale a
//     quello misurato durante il tuning. Corretto anche il ripiego della
//     variante quando la funzione base non rientra nella deadline (vedi
//     selectWithWeights).
//  19. [DECISO 15/11] CO2 senza energia idle, coerente con il paper: l'Eq. 10
//     (CO^f = ξ_i·E_exec + ξ_j·E_data) e l'Alg. 2 (riga 4) usano solo l'energia
//     di esecuzione e dei dati; ε_i compare solo nell'energia (Eq. 7 -> Eq. 8,
//     9 -> termine G della funzione obiettivo). Prima il codice moltiplicava
//     per ξ_i l'energia totale, idle compresa: con potenze idle molto diverse
//     tra nodi (es. cloud 300 W, edge 40 W) cambiava il confronto sul termine
//     CO2. L'energia (energyJoules) restava invariata, idle compreso e scalato
//     per il tempo (decisione del 22/10); dal 20/11 anche l'energia è senza
//     idle (punto 22).
//  20. [NUOVO 16/11] Misure del tuning salvate su file (energyless_tuning.go):
//     scripts/tune-energyless.sh scrive tempi di esecuzione, di init e
//     probabilità di cold start per funzione e tipo di nodo (edge/cloud) in un
//     file YAML, indicato dalla chiave tuning.file nel file della policy.
//     Servono perché le metriche Prometheus si perdono al riavvio di
//     Prometheus/Pushgateway e, per gli edge, sono legate alla chiave del nodo
//     (che cambia a ogni riavvio). Ordine: metrica live -> tuning da file ->
//     default (punto 18). Per il cloud il file evita anche la probabilità di
//     cold start 0 quando mancano le metriche.
//  21. [DECISO 18/11 con il prof. Russo Russo, applicato 20/11] Task che non
//     rispetta più la deadline residua: non si riassegna alla coppia più
//     veloce sperando di recuperare (supera il ripiego selectFastest del
//     punto 17), si mantiene il piano senza modificarlo. In buildPlan(), se
//     nessuna coppia rispetta la deadline residua, il task riceve la coppia
//     che sceglierebbe comunque la funzione obiettivo con gli stessi pesi,
//     togliendo solo il vincolo di deadline (capacità, banda e soglia di
//     accuratezza restano vincoli). Il piano a rischio resta segnalato nel log
//     una volta per richiesta; se la richiesta sfora davvero lo conteranno i
//     contatori delle funzioni completate entro la deadline.
//  22. [DECISO 18/11 con il prof. Russo Russo, applicato 20/11] Modello
//     energetico della funzione obiettivo:
//     - la potenza idle ε_i NON entra più nell'energia: i nodi restano
//     accesi a prescindere dalle decisioni di scheduling, quindi l'idle non
//     cambia la scelta (supera la decisione del 22/10, punto 5). Resta
//     idleEnergyJoules() solo per calcolare a posteriori il consumo totale
//     negli esperimenti. Di conseguenza la CO2 (punto 19) si calcola
//     sull'energia così com'è, senza sottrarre nulla;
//     - ogni funzione usa 1 core CPU: energia di esecuzione = potenza di un
//     core × tempo di esecuzione, al posto della scalatura memoria/2048 ×
//     CPUDemand del modello di Cecilia (internal/emissions). La chiave
//     consumption.processing.power è la potenza del NODO ("processing power
//     consumed by nodes", internal/regions/types.go), quindi la potenza di
//     un core è quella diviso il numero di core del nodo: pool.cpus in
//     locale (node.LocalResources.TotalCPUs()), TotalCPU dell'heartbeat per i
//     vicini, cloud.cores del file della policy per il cloud (default 1).
//  23. [DECISO 18/11 con il prof. Russo Russo, applicato 20/11] Trasferimento
//     dati nodo a nodo. Nel main di Serverledge (commit fc93e84 di Matteo
//     Pallagrosi) i dati non passano più per etcd: viaggiano nel corpo delle
//     richieste HTTP di offloading (WorkflowInvocationResumeRequest.Data) e
//     tornano nella risposta (ResumeData.OutputData). Il nodo remoto riceve
//     sempre CanDoOffloading=false e, appena incontra un task fuori dal suo
//     ToExecute o la fine del workflow, restituisce dati e risultato
//     all'ORIGINE: i trasferimenti sono diretti ma con topologia a stella
//     centrata sull'origine. Quindi, per un task posto sul nodo X il cui input
//     si trova sul nodo Y (dove è stato eseguito il predecessore, o l'origine
//     per il primo task):
//     - X == Y: tempo, energia e costo di trasferimento nulli (decisione del
//     18/11: task consecutivi sullo stesso nodo non costano nulla);
//     - X != Y: tratta Y -> origine (se Y non è l'origine) più tratta
//     origine -> X (se X non è l'origine);
//     - l'ultimo task funzione del workflow, se è su X != origine, paga anche
//     il ritorno del risultato X -> origine.
//     Per ogni tratta: tempo = byte / banda, con le stesse bande della
//     RemoteOffloadingPolicy del main (edge_to_edge_bandwidth tra nodi edge,
//     node2datastore.bandwidth tra un nodo edge e il cloud); energia = byte ×
//     energia di trasmissione del mittente + byte × energia di ricezione del
//     destinatario, ciascuna con la CO2Intensity del proprio nodo; costo =
//     byte × costo di trasferimento per byte (cloud2datastore.transfer.cost se
//     la tratta tocca il cloud, node2datastore.transfer.cost tra nodi edge).
//     Byte: dimensione media dell'output del predecessore (AvgOutputSize),
//     ParamsSize della richiesta per il primo task. Il costo di storage
//     (CS^f) è tolto: non c'è più un data store (e Cecilia il 17/11 aveva già
//     indicato di ometterlo). Corretta anche l'unità dell'energia di rete del
//     cloud: il file delle regioni ha lo stesso valore di consumption.energy.tx
//     dei nodi (nJ/byte), che node.Resources divide per 1e9, mentre prima per
//     il cloud veniva usato senza dividere.
//  24. [21/11] Fork-join (ParallelTask del main, lavoro di Matteo Pallagrosi).
//     Prima buildPlan() seguiva solo GetNext() dei task unari e si fermava al
//     ParallelTask: i rami e tutto ciò che segue il join restavano fuori dal
//     piano (eseguiti in locale). Ora:
//     - la visita segue anche GetBranches() dei FanOutTask, in ordine
//     topologico: un task (in particolare il join) si pianifica quando sono
//     stati pianificati tutti i suoi predecessori;
//     - tempi: l'inizio stimato di un task è il massimo tra le fine stimate
//     dei predecessori, il budget di deadline è la deadline residua meno
//     quell'inizio, la durata stimata del workflow è il percorso più lungo
//     (sostituisce l'accumulatore lineare elapsedChain del punto 13, che
//     su una catena dà lo stesso risultato);
//     - dati: un join riceve un input per ramo; zero trasferimenti se tutti i
//     rami stanno sullo stesso nodo del join, altrimenti ogni output remoto
//     torna all'origine e da lì va al nodo del join (il motore restituisce
//     il controllo all'origine finché il join non è eseguibile). I task
//     strutturali (Pass, ParallelTask) non entrano mai nel piano remoto e
//     girano sull'origine, quindi i dati ci passano (viaOrigin);
//     - decisioni: Evaluate() restituisce una decisione per ogni nodo remoto
//     con task pronti (Invoke del main le accetta tutte ed esegue in locale
//     i task pronti non offloadati), al posto di un solo nodo per round con
//     "il locale vince" di ILP/HEFTless, che serializzava i rami.
//     Doppio conteggio (corretto il 21/11): un trasferimento che serve a più
//     task (l'output del Fork inviato a un nodo che esegue due rami, o
//     l'output di un task remoto restituito all'origine e usato da più
//     successori) avviene una volta sola, quindi energia, CO2 e costo della
//     tratta si contano una volta (mappa shipped in buildPlan); il tempo
//     invece resta per ogni task, perché ciascuno aspetta l'arrivo dei dati.
//     Limiti noti: due rami sullo stesso nodo sono stimati come se non si
//     contendessero le risorse (tabella delle prenotazioni, punto 7 della
//     lista del 18/11); un ChoiceTask dentro un ramo ferma la visita, quindi
//     il join e i task successivi restano fuori dal piano fino alla
//     ripianificazione dopo il ChoiceTask (punto 27).
//  25. [21/11] Pulizia della cache dei piani. Ogni richiesta salvava il suo
//     piano in policy.cache con un TTL di placement.ttl letture (9999), che
//     una richiesta non consuma mai: la voce restava in memoria per sempre e
//     in un esperimento lungo la cache cresceva senza limite. Ora la voce
//     viene eliminata quando la richiesta termina sul nodo di origine, con
//     successo o con errore, tramite l'interfaccia opzionale
//     RequestCompletionListener (offloading_policy.go), chiamata da
//     Workflow.Invoke() (workflow.go) solo se r.CanDoOffloading. Le altre
//     policy non la implementano e non cambiano comportamento.
//  26. [DECISO 18/11 con il prof. Russo Russo, applicato 22/11] Due contatori
//     delle richieste di workflow pianificate da questo nodo: (1) completate
//     (workflow terminato senza errori) e (2) completate rispettando anche la
//     deadline (tempo di risposta <= QoS.MaxRespT, misurato come fa l'API da
//     r.Arrival). Le richieste con errore sono contate a parte. Aggiornati in
//     RequestCompleted() (RequestCompletionListener, che dal 22/11 riceve
//     anche l'errore di Invoke) e scritti nel log a ogni richiesta; Stats()
//     li restituisce. Ripartono da zero a ogni avvio del nodo. La deadline è
//     della richiesta, non della singola funzione: per questo si contano le
//     richieste (come indicato anche da Cecilia l'11/11).
//  27. [APPROVATO 18/11 dal prof. Russo Russo, applicato 22/11]
//     Ripianificazione dopo un ChoiceTask. Il ChoiceTask, come gli altri task
//     strutturali, non entra mai nel piano remoto: lo esegue l'origine, che
//     sceglie il ramo e segna Skipped i task dei rami non scelti. La visita
//     di buildPlan() si ferma al ChoiceTask, quindi prima i task del ramo
//     scelto restavano fuori dal piano e giravano in locale. Ora la cache
//     ricorda anche i task visitati: quando tra i task pronti ce n'è uno mai
//     visitato (il primo del ramo scelto), evaluate() pianifica solo da lì
//     in avanti e aggiunge il risultato al piano (mergePlans), senza toccare
//     i task già pianificati (coerente col punto 21). La deadline residua si
//     ricalcola dall'istante della ripianificazione; la soglia di accuratezza
//     parte dal prodotto delle accuratezze già scelte (chosenAccuracy); le
//     varianti dei nuovi task si aggiungono a r.Variants, che
//     FunctionTask.resolveVariant ora legge sotto r.mu. Join dopo un fork con
//     un ChoiceTask in un ramo: un task si pianifica solo quando tutti i suoi
//     predecessori nel grafo sono eseguiti, scartati, già pianificati o
//     raggiunti dalla visita; altrimenti (predecessore dietro un ChoiceTask
//     non ancora eseguito) resta fuori dal piano con tutto ciò che lo segue.
//     Così il join viene pianificato solo nella ripianificazione dopo il
//     ChoiceTask, con gli input di tutti i rami: quelli già pianificati
//     prima arrivano dal nodo scelto allora (knownPlacement), il loro tempo
//     non ritarda il join (già passato o in parallelo).
//  28. [DECISO 18/11 con il prof. Russo Russo, applicato 23/11] Tabella delle
//     risorse prenotate. Le risorse dei vicini arrivano con nearbyMonitoring
//     ogni registry.nearby.interval (20 s di default): richieste ravvicinate,
//     e i rami paralleli di una stessa richiesta, vedevano lo stesso nodo
//     libero. Inoltre ogni richiesta di workflow gira nella propria goroutine
//     e chiama Evaluate() sulla stessa istanza della policy: due richieste
//     potevano pianificare nello stesso istante. Ora:
//     - planMu serializza la pianificazione (primo piano e ripianificazioni
//     dopo un ChoiceTask): come uno scheduler unico, una richiesta alla
//     volta legge lo stato e prenota;
//     - ogni task posizionato su un nodo edge prenota memoria e CPU della
//     funzione scelta nell'intervallo stimato [inizio, fine) (il cloud non
//     si prenota: capacità infinita);
//     - la capacità di un candidato per un task è lo stato letto meno le
//     prenotazioni attive all'istante di inizio stimato del task: i task in
//     catena non si sommano (riusano il core), i rami paralleli e le
//     richieste concorrenti sì;
//     - a ogni pianificazione si tolgono le prenotazioni scadute, quelle già
//     iniziate sul nodo locale (stato in tempo reale) e quelle iniziate
//     prima dell'ultimo heartbeat del vicino (lo stato le include già):
//     restano solo quelle future; RequestCompleted toglie quelle della
//     richiesta terminata.
//     Limite: la tabella è del nodo che pianifica; richieste ricevute da nodi
//     di origine diversi verso lo stesso vicino si vedono solo tramite
//     l'heartbeat.
//     [CONFERMATO 24/11 dal prof. Russo Russo] (a) il mutex va bene: conta
//     che pianificazione e prenotazione siano atomiche, non l'ordine di
//     arrivo (una coda FIFO stretta non cambierebbe i risultati); anche la
//     ripianificazione dopo un ChoiceTask prende planMu (evaluate: lock se
//     manca il piano o ci sono task pronti mai visitati). (b) Reset parziale
//     preferito al reset totale. (c) De-allocazione a fine richiesta, anche
//     con errore: il defer in Workflow.Invoke() è registrato prima di ogni
//     return e RequestCompleted rilascia le prenotazioni prima di guardare
//     l'esito. (d) Negli esperimenti più nodi di ingresso nella stessa zona;
//     la condivisione delle prenotazioni tra nodi di origine NON si gestisce
//     (richiederebbe coordinamento): sviluppo futuro. Da rivedere con la
//     prof.ssa Cardellini.
package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/metrics"
	"github.com/serverledge-faas/serverledge/internal/node"
	"github.com/serverledge-faas/serverledge/internal/regions"
	"github.com/serverledge-faas/serverledge/internal/registration"
)

// ---------------------------------------------------------------------
// Parametri di configurazione di EnergyLessPolicy, definiti in
// internal/config/keys.go (WORKFLOW_OFFLOADING_POLICY_ENERGYLESS_*):
//   - pesi dell'obiettivo (Eq. 22): WEIGHT_TIME / WEIGHT_ENERGY /
//     WEIGHT_CARBON / WEIGHT_COST;
//   - PLACEMENT_TTL: numero di letture consentite al piano in cache;
//   - costo di trasferimento (CT^f, Eq. 14) e di storage (CS^f, Eq. 15) verso
//     il data store: NODE_TO_DATA_STORE_TRANSFER_COST /
//     CLOUD_TO_DATA_STORE_TRANSFER_COST e NODE_TO_DATA_STORE_STORAGE_COST /
//     CLOUD_TO_DATA_STORE_STORAGE_COST (default 0.0).
//
// La policy è registrata in workflow.go::CreateOffloadingPolicy() (branch
// "energyless") — vedi anche il blocco in fondo al file.
//
// Banda, costo di regione, potenze e CO2 riusano chiavi già esistenti
// (region.cost, node2datastore.bandwidth, cloud2datastore.bandwidth,
// consumption.processing.power/.energy.tx/.rx, regions.file.path).
//
// [NUOVO 06/11, suggerimento di Cecilia] I parametri della policy (pesi,
// placement TTL, soglia di accuratezza di default, costi verso il data store)
// possono stare in un file YAML dedicato, indicato dalla chiave
// WORKFLOW_OFFLOADING_POLICY_ENERGYLESS_CONFIG_PATH (vedi energyless_config.go
// ed examples/energyless/energyless-config.yaml). Priorità: file > chiavi
// principali sopra elencate (fallback) > default nel codice. La soglia minima
// di accuratezza è defaultMinAccuracy (0.95) oppure il valore del file
// (default.min.accuracy), sovrascrivibile per richiesta con
// RequestQoS.MinAccuracy (--min-accuracy da CLI).
// ---------------------------------------------------------------------

// energyLessWeights sono i pesi γ1..γ4 dell'Eq. 22 del paper (tempo,
// energia, CO2, costo). Se non sommano a 1 non importa: contano solo i
// pesi relativi, dato che l'obiettivo è comunque normalizzato per task.
type energyLessWeights struct {
	time, energy, carbon, cost float64
}

// energyLessCacheEntry è il piano cachato per una singola richiesta: il
// placement calcolato una sola volta da buildPlan() per tutta la catena di
// task non ancora eseguiti, più un contatore di letture residue. Verificato
// sul codice reale (cachedPlacement in remote_offloading_policy.go, usata da
// ILP/HEFTless): il "ttl" NON è una durata in tempo reale (come avevo
// assunto prima di vedere il file), ma un contatore di quante volte ancora
// la entry può essere letta da getCachedSolution() prima di scadere — si
// decrementa di 1 a ogni lettura riuscita ed è rimosso quando arriva a 0.
// Qui replicato con lo stesso comportamento (vedi getCachedPlan).
type energyLessCacheEntry struct {
	placement map[TaskId]*elEstimate
	ttl       int
	// visited: task raggiunti dalla visita di buildPlan() (anche quelli non
	// posizionati: strutturali, fuori dal piano). Un task pronto che non c'è
	// è il primo di un ramo scelto da un ChoiceTask dopo la pianificazione:
	// fa scattare la ripianificazione incrementale (punto 27).
	visited map[TaskId]bool
}

// EnergyLessPolicy implementa OffloadingPolicy (offloading_policy.go)
// traducendo l'Algoritmo 1 di EnergyLess (ranking + assegnazione greedy) in
// un piano calcolato una sola volta per richiesta e cachato (vedi punto 6
// in testa al file), invece che su un batch di workflow.
type EnergyLessPolicy struct {
	weights    energyLessWeights
	regionsCfg *regions.Config // caricato una volta in Init(), può restare nil se non configurato

	cacheMu      sync.Mutex
	cache        map[string]*energyLessCacheEntry // chiave: r.Id — valori puntatore come in placementCache (ILP/HEFTless), per poter decrementare ttl in-place
	cacheTTLInit int                              // numero di letture consentite prima della scadenza (vedi energyLessCacheEntry)

	// Parametri risolti in Init() da loadEnergyLessParams()
	// (energyless_config.go): file YAML dedicato > chiavi principali > default.
	defaultMinAccuracy float64 // soglia usata se la richiesta non ha MinAccuracy
	nodeTransferCost   float64 // costo/byte verso il data store, nodo locale/edge
	cloudTransferCost  float64 // idem, cloud
	nodeStorageCost    float64 // [20/11, punto 23] letto ma non più usato: niente costo di storage
	cloudStorageCost   float64 // idem

	// Tempi di ripiego senza metriche Prometheus (punto 18): da
	// default.exec.time / default.init.time nel file YAML della policy,
	// altrimenti defaultFallbackTime.
	defaultExecTime float64
	defaultInitTime float64

	// Misure del tuning lette da file (punto 20): usate quando manca la
	// metrica live, prima dei default.
	tuning energyLessTuning

	// Core di un nodo cloud (punto 22): da cloud.cores nel file della policy,
	// altrimenti defaultCloudCores.
	cloudCores float64

	// [punto 28] planMu serializza la costruzione dei piani (primo piano e
	// ripianificazioni): come lo scheduler unico, una richiesta alla volta
	// legge lo stato e prenota le risorse. resMu protegge la tabella delle
	// prenotazioni, letta anche da RequestCompleted.
	planMu       sync.Mutex
	resMu        sync.Mutex
	reservations []elReservation

	// Contatori delle richieste pianificate da questo nodo (punto 26),
	// aggiornati da RequestCompleted; atomici perché le richieste terminano
	// in goroutine diverse. Ripartono da zero a ogni avvio del nodo.
	stats struct {
		completed      atomic.Uint64
		withinDeadline atomic.Uint64
		failed         atomic.Uint64
	}

	// cloudCO2Mu/lastKnownCloudCO2/hasLastKnownCloudCO2: fallback per la
	// query live della CO2Intensity del cloud (vedi gatherCandidates()).
	// Confermato da Cecilia via mail (16/10) che il fallback "ingenuo" sulla
	// CO2Intensity del nodo locale, copiato da discoverCloudCandidates() in
	// internal/scheduling/co2_qos_policy.go, NON è una scelta di design
	// validata — è semplicemente un aspetto mai sistemato in quel codice.
	// Qui si usa invece l'ultimo valore live ottenuto con successo dal
	// cloud, cacheato in memoria per la vita del processo (non per singola
	// richiesta: è indipendente da cache/cacheTTLInit, che riguardano il
	// placement, non la CO2). Lock separato da cacheMu perché gatherCandidates()
	// gira fuori dal lock del placement (vedi Evaluate(): getCachedPlan()
	// rilascia cacheMu prima di chiamare gatherCandidates()), quindi non c'è
	// rischio di deadlock riusando lo stesso Mutex, ma un lock dedicato
	// rende la relazione esplicita e a prova di futuri refactor.
	cloudCO2Mu           sync.Mutex
	lastKnownCloudCO2    float64
	hasLastKnownCloudCO2 bool
}

func (policy *EnergyLessPolicy) Init() {
	// Parametri: file YAML dedicato (se configurato) > chiavi principali >
	// default — vedi energyless_config.go.
	params := loadEnergyLessParams()
	policy.weights = params.weights
	policy.defaultMinAccuracy = params.defaultMinAccuracy
	policy.defaultExecTime = params.defaultExecTime
	policy.defaultInitTime = params.defaultInitTime
	policy.tuning = params.tuning
	policy.cloudCores = params.cloudCores
	policy.nodeTransferCost = params.nodeTransferCost
	policy.cloudTransferCost = params.cloudTransferCost
	policy.nodeStorageCost = params.nodeStorageCost
	policy.cloudStorageCost = params.cloudStorageCost

	if regionsFile := config.GetString(config.REGIONS_FILE_PATH, ""); regionsFile != "" {
		cfg, err := regions.Load(regionsFile)
		if err != nil {
			log.Printf("EnergyLess: could not load regions file %q: %v", regionsFile, err)
		} else {
			policy.regionsCfg = cfg
		}
	}

	policy.cache = make(map[string]*energyLessCacheEntry)
	// NOTA: come in ILP/HEFTless, "ttl" è un numero di LETTURE residue (si
	// decrementa a ogni round che riusa il piano in cache), non una durata
	// in secondi — vedi commento su energyLessCacheEntry.
	//
	// [CORRETTO dopo lettura di ilp_offloading_policy.go/heftless_policy.go
	// reali]: config.GetInt() esiste davvero (non serve più il cast da
	// GetFloat), usata qui direttamente. Anche il DEFAULT è stato corretto:
	// il valore reale passato a cacheSolution() è
	//   - ILP:      config.GetInt(WORKFLOW_OFFLOADING_POLICY_ILP_PLACEMENT_TTL, 2) - 1
	//     cioè default effettivo 1 lettura: ILP ricalcola il piano quasi a
	//     ogni round, coerente col fatto che l'ILP è pensato per rivalutare
	//     dinamicamente la schedulazione.
	//   - HEFTless: cacheSolution(r, &placement, 9999) — costante fissa,
	//     cioè "praticamente per sempre" entro una singola richiesta.
	// EnergyLess, per decisione del 02/10, deve comportarsi come HEFTless su
	// questo punto (piano calcolato una sola volta, "la rivalutazione
	// dinamica del piano resta rimandata", niente re-scheduling a metà
	// esecuzione — vedi punto 6 in testa al file): il default qui va quindi
	// allineato a quello di HEFTless (9999), non a quello di ILP. Il default
	// precedente (5) era arbitrario e avrebbe forzato un ricalcolo indesiderato
	// a metà di richieste con più di 5 round.
	policy.cacheTTLInit = params.placementTTL
}

// elCandidate è l'equivalente locale di una "istanza" I del paper: un nodo
// su cui l'attuale task pronto potrebbe essere eseguito (locale, un vicino
// edge, o il target cloud unico configurato per l'offloading verticale).
type elCandidate struct {
	key              string // registration key del nodo, oppure "CLOUD"
	apiURL           string // usato per popolare OffloadingDecision.RemoteHost
	isLocal          bool
	area             string
	availMemMB       float64 // non usato per il candidato CLOUD, vedi estimateOnCandidate
	statusTime       int64   // [punto 28] istante (Unix, s) dello stato letto: heartbeat del vicino, ora per il nodo locale
	availCPU         float64 // idem — per CLOUD si assume capacità infinita (deciso 02/10)
	co2Intensity     float64 // gCO2eq/kWh — ξ_i nel paper
	procPowerW       float64 // W — τ_i nel paper: potenza sotto carico dell'intero nodo (consumption.processing.power)
	cores            float64 // numero di core del nodo, per la potenza di un core (punto 22); <= 0 = sconosciuto
	idlePowerW       float64 // W — ε_i nel paper (potenza a riposo). [20/11, punto 22] Non entra più nella funzione obiettivo: resta per il consumo totale a posteriori (idleEnergyJoules)
	txEnergyJPerByte float64 // J/byte — energia NIC in trasmissione
	rxEnergyJPerByte float64 // J/byte — energia NIC in ricezione
}

// elEstimate è l'equivalente locale dell'output di EST() (Alg. 2 del paper):
// le 4 metriche stimate per una coppia (task-o-variante, candidato) più
// l'obiettivo pesato e normalizzato (Eq. 22).
type elEstimate struct {
	candidate      elCandidate
	functionName   string        // nome della funzione o VARIANTE effettivamente valutata (punto 1)
	completionTime time.Duration // TT^f, Eq. 2-4
	energyJoules   float64       // E^f, Eq. 7: esecuzione (1 core) + dati, senza ε_i idle dal 20/11 (punto 22)
	carbonGrams    float64       // CO^f, Eq. 10 (senza ε_i idle dal 15/11, punto 19; termine storage ξ_j: approssimato con ξ_i, confermato corretto 22/10 — vedi estimateOnCandidate)
	cost           float64       // Z^f = CC^f + CT^f + CS^f, Eq. 13+14+15
	accuracy       float64       // ∈[0,1], [NUOVO 30/10] accuratezza della funzione/variante scelta rispetto a quella richiesta dal task: 1.0 per la funzione del task, Function.Utility per una variante sostituta (vedi selectBest)
	objective      float64       // somma pesata normalizzata, Eq. 22
	// timeSource: da dove viene il tempo di esecuzione stimato — "misura"
	// (metrica di Prometheus, via Pushgateway), "tuning" (file del tuning,
	// punto 20), "speedup" (variante senza misura: tempo della base diviso
	// SpeedUp) o "default" (default.exec.time). Mostrato nel log del piano.
	timeSource string
}

// defaultCloudCores è il numero di core di un nodo cloud quando il file della
// policy non specifica cloud.cores (punto 22): 1 significa "potenza del nodo
// intera per funzione", la stima più prudente.
const defaultCloudCores = 1.0

// defaultMinAccuracy è la soglia minima di accuratezza del workflow usata
// quando la richiesta non ne specifica una (RequestQoS.MinAccuracy <= 0).
// [NUOVO 30/10, deciso nel ricevimento del 22/10] Valore provvisorio al 95%
// scelto "per il momento": promuoverlo a chiave di
// configurazione (come i pesi γ1..γ4) se servirà variarlo negli esperimenti.
const defaultMinAccuracy = 0.95

// defaultFallbackTime (secondi) è il ripiego generico per tempo di esecuzione
// e di init quando per una coppia (nodo, funzione) non c'è ancora nessuna
// misura e il file della policy non indica default.exec.time /
// default.init.time (punto 18). Stesso valore usato finora.
const defaultFallbackTime = 0.01

// fallbackExecTime e fallbackInitTime ritornano i tempi di ripiego
// configurati, con defaultFallbackTime se la policy non è stata
// inizializzata con Init() (es. nei test).
func (policy *EnergyLessPolicy) fallbackExecTime() float64 {
	if policy.defaultExecTime > 0 {
		return policy.defaultExecTime
	}
	return defaultFallbackTime
}

func (policy *EnergyLessPolicy) fallbackInitTime() float64 {
	if policy.defaultInitTime > 0 {
		return policy.defaultInitTime
	}
	return defaultFallbackTime
}

// accuracyEpsilon assorbe gli errori di arrotondamento in virgola mobile nel
// confronto tra prodotti di accuratezze e soglia (es. 0.95*1.0 vs 0.95).
const accuracyEpsilon = 1e-9

// minAccuracyThreshold ritorna la soglia minima di accuratezza complessiva del
// workflow per la richiesta r, sempre in (0,1]: il campo MinAccuracy della QoS
// se impostato (> 0), altrimenti defaultMinAccuracy. Un valore > 1 è
// irraggiungibile (l'accuratezza è un prodotto di fattori ≤ 1) e viene
// limitato a 1, cioè "solo funzioni base, nessuna variante degradata".
func (policy *EnergyLessPolicy) minAccuracyThreshold(r *Request) float64 {
	t := r.QoS.MinAccuracy
	if t <= 0 {
		// Soglia di default: file YAML dedicato > costante defaultMinAccuracy
		// (anche se la policy non è passata da Init()).
		t = policy.defaultMinAccuracy
		if t <= 0 {
			t = defaultMinAccuracy
		}
	}
	if t > 1 {
		t = 1
	}
	return t
}

// evaluate prende la richiesta attuale (Request) e lo stato di avanzamento
// (Progress) e restituisce le decisioni di offloading del round (una per nodo
// remoto con task pronti, punto 24) e un eventuale errore.
func (policy *EnergyLessPolicy) evaluate(r *Request, p *Progress) ([]OffloadingDecision, error) {
	noOffload := []OffloadingDecision{{Offload: false}}
	if p == nil || !r.CanDoOffloading || len(p.ReadyToExecute) == 0 {
		return noOffload, nil
	}

	// Piano in cache (punto 6): se esiste già un placement valido per questa
	// richiesta, lo si usa direttamente senza ricalcolare nulla.
	residualDeadline := func() float64 {
		if r.QoS.MaxRespT > 0.0 {
			return r.QoS.MaxRespT - time.Since(r.Arrival).Seconds()
		}
		return math.Inf(1)
	}

	placement, visited, ok := policy.getCachedPlan(r.Id)
	if !ok || len(unvisitedReadyTasks(p, visited)) > 0 {
		// [punto 28] Una pianificazione alla volta (vedi planMu): le
		// richieste concorrenti aspettano qui, come in uno scheduler unico.
		// La voce in cache di QUESTA richiesta non cambia mentre si aspetta,
		// perché Evaluate gira sotto r.mu.
		policy.planMu.Lock()
		defer policy.planMu.Unlock()
		policy.pruneReservations(time.Now())
	}
	if !ok {
		candidates := policy.gatherCandidates()
		if len(candidates) == 0 {
			return noOffload, nil
		}
		// Calcola il placement per TUTTI i task non ancora eseguiti
		// raggiungibili (catene e rami di fork-join, punto 24), una sola
		// volta, e lo cachea.
		placement, visited = policy.buildPlan(r, p, candidates, residualDeadline(), p.ReadyToExecute, 1.0, nil, nil)
		policy.cacheSolution(r.Id, placement, visited)
	} else if fresh := unvisitedReadyTasks(p, visited); len(fresh) > 0 {
		// [NUOVO 22/11, punto 27 — approvato il 18/11 dal prof. Russo Russo]
		// Ripianificazione dopo un ChoiceTask: la visita del piano si era
		// fermata al ChoiceTask (il ramo si conosce solo a runtime). Ora il
		// ChoiceTask è stato eseguito sull'origine, i rami non scelti sono
		// Skipped e il primo task del ramo scelto è pronto ma non è mai stato
		// visitato. Si pianifica SOLO da lì in avanti e si AGGIUNGE al piano:
		// i task già pianificati non cambiano (coerente col punto 21). La
		// deadline residua si ricalcola da adesso; la soglia di accuratezza
		// parte dal prodotto delle accuratezze già scelte.
		candidates := policy.gatherCandidates()
		if len(candidates) > 0 {
			added, addedVisited := policy.buildPlan(r, p, candidates, residualDeadline(), fresh, chosenAccuracy(p, placement), visited, placement)
			placement, visited = mergePlans(placement, visited, added, addedVisited)
			policy.cacheSolution(r.Id, placement, visited)
			log.Printf("EnergyLess: richiesta %s, ripianificazione dopo un ChoiceTask dai task %v: %d task aggiunti al piano (totale %d)",
				r.Id, fresh, len(added), len(placement))
		}
	}

	// Varianti scelte (punto 16). Al primo piano si crea la mappa; dopo una
	// ripianificazione (punto 27) si AGGIUNGONO le varianti dei nuovi task,
	// senza cambiare quelle esistenti. Evaluate() gira con r.mu bloccato e
	// FunctionTask.resolveVariant legge la mappa sotto lo stesso lock, quindi
	// non ci sono accessi concorrenti. variantChoices ritorna sempre una mappa
	// non nil, anche se vuota.
	if r.Variants == nil {
		r.Variants = variantChoices(r.W, placement)
	} else {
		for tid, v := range variantChoices(r.W, placement) {
			if _, set := r.Variants[tid]; !set {
				r.Variants[tid] = v
			}
		}
	}

	return policy.computeDecisionsFromPlacement(p, placement, r.Variants), nil
}

// Evaluate implementa l'interfaccia OffloadingPolicy introdotta con fork-join:
// Invoke accetta più decisioni (una per destinazione remota) ed esegue in locale
// tutti i task pronti non inclusi in una decisione di offload.
// runningTasks non serve: il motore offloada solo i task ancora in
// ReadyToExecute, quindi quelli già in esecuzione non vengono ripresi.
func (policy *EnergyLessPolicy) Evaluate(r *Request, p *Progress, runningTasks map[TaskId]bool) ([]OffloadingDecision, error) {
	return policy.evaluate(r, p)
}

// variantChoices ricava dal piano le sostituzioni "task -> variante": un task
// compare solo se il piano ha scelto una funzione diversa da quella del task.
func variantChoices(w *Workflow, placement map[TaskId]*elEstimate) map[TaskId]string {
	choices := make(map[TaskId]string)
	for tid, est := range placement {
		ft, isFunc := w.Tasks[tid].(*FunctionTask)
		if !isFunc || est == nil {
			continue
		}
		if est.functionName != "" && est.functionName != ft.Func {
			choices[tid] = est.functionName
		}
	}
	return choices
}

// buildPlan calcola il placement per l'intera catena di task non ancora
// eseguiti, a partire dai task pronti del round corrente, camminando
// FunctionTask.NextTask finché la catena resta lineare (deciso 02/10: solo
// strutture a catena sequenziale per ora). Si ferma al primo task
// non-FunctionTask (es. ChoiceTask la cui diramazione non è ancora nota),
// quando NextTask è vuoto, o se un task è già stato visitato (protezione
// da cicli).
func (policy *EnergyLessPolicy) buildPlan(r *Request, p *Progress, candidates []elCandidate, residualDeadline float64, startTasks []TaskId, startAccuracy float64, known map[TaskId]bool, knownPlacement map[TaskId]*elEstimate) (map[TaskId]*elEstimate, map[TaskId]bool) {
	placement := make(map[TaskId]*elEstimate)
	// Istante di riferimento del piano: startAt e completionTime sono relativi
	// a questo, le prenotazioni (punto 28) sono in tempo assoluto.
	planTime := time.Now()

	// Ranking dei task pronti (righe 6-12, Alg. 1): r_f = tempo medio
	// stimato sulle istanze candidate. Dà l'ordine con cui si visitano i
	// task pronti nello stesso istante (per esempio i rami di un fork).
	// NOTA: non propaga ancora il rank dei predecessori (riga 11).
	// startTasks: al primo piano i task pronti; dopo un ChoiceTask (punto
	// 27) solo i task pronti mai visitati (il ramo scelto).
	ready := policy.rankReadyTasks(r, startTasks, candidates)

	// [NUOVO 21/11, punto 24] Visita in ordine topologico del grafo da
	// pianificare: si seguono GetNext() dei task unari e GetBranches() dei
	// FanOutTask (ParallelTask); ci si ferma davanti a un ConditionalTask
	// (ChoiceTask: il ramo dipende da dati a runtime) e alla fine del
	// workflow. Un task entra nella visita solo quando sono stati visitati
	// tutti i suoi predecessori raggiungibili, così il join di un fork viene
	// pianificato dopo l'ultimo dei suoi rami. pending[t] conta i predecessori
	// di t ancora da visitare.
	pending := make(map[TaskId]int)
	reached := make(map[TaskId]bool)
	toVisit := append([]TaskId(nil), ready...)
	for len(toVisit) > 0 {
		tid := toVisit[0]
		toVisit = toVisit[1:]
		if reached[tid] {
			continue
		}
		reached[tid] = true
		for _, s := range planSuccessors(r.W.Tasks[tid]) {
			pending[s]++
			toVisit = append(toVisit, s)
		}
	}

	// startAt: istante di inizio stimato di ogni task, misurato dall'inizio
	// della pianificazione = massimo tra le fine stimate dei predecessori
	// (Eq. 4 estesa ai rami paralleli). Sostituisce l'accumulatore lineare
	// elapsedChain del punto 13: su una catena dà lo stesso risultato, su un
	// fork i rami partono insieme e il join aspetta il ramo più lento. Il
	// budget di deadline di un task è residualDeadline - startAt[tid];
	// makespan è la durata stimata del workflow (percorso più lungo).
	startAt := make(map[TaskId]float64)
	makespan := 0.0

	// [NUOVO 30/10, deciso nel ricevimento del 22/10] Soglia minima di
	// accuratezza: l'accuratezza complessiva del workflow è il PRODOTTO delle
	// accuratezze dei task (∈[0,1]), anche di quelli su rami paralleli, e
	// deve restare >= minAccuracy. accChain è il prodotto dei task già
	// posizionati in QUESTA chiamata. Poiché la funzione del task ha sempre
	// accuratezza 1.0 (vedi selectBest), richiedere accChain * acc(task) >=
	// soglia a ogni passo basta a garantire il prodotto finale. Limite noto:
	// la scelta resta greedy per task (come l'Alg. 1), vedi punto 8 della
	// lista del 18/11 (fattore correttivo).
	minAccuracy := policy.minAccuracyThreshold(r)
	// startAccuracy: 1 al primo piano; in una ripianificazione (punto 27) il
	// prodotto delle accuratezze già scelte, così il vincolo resta sul
	// prodotto dell'intero workflow.
	accChain := startAccuracy

	// deadlineAtRisk: il piano prevede già di sforare la deadline (punti 17 e
	// 21), usato solo per loggarlo una volta per piano.
	deadlineAtRisk := false

	// [punto 23, esteso il 21/11 col punto 24] Dove si trovano gli input di
	// ciascun task e quanto pesano, per stimare i trasferimenti nodo a nodo.
	// Un join ha un input per ogni ramo. I task pronti all'inizio della
	// visita hanno l'input sull'origine: lo StartTask con i parametri della
	// richiesta, un task pronto a metà esecuzione con l'output dei
	// predecessori, già riportato all'origine.
	origin := originCandidate(candidates)
	inputsOf := make(map[TaskId][]elDataSource)
	for _, tid := range ready {
		// item univoco per ogni task pronto: il suo input non è condiviso con
		// altri task di questa visita.
		item := "input:" + tid
		if _, isStart := r.W.Tasks[tid].(*StartTask); isStart {
			inputsOf[tid] = []elDataSource{{loc: origin, bytes: float64(r.ParamsSize), item: item}}
		} else {
			inputsOf[tid] = []elDataSource{{loc: origin, bytes: predecessorOutputBytes(r.W, tid), item: item}}
		}
	}
	// Tratte già pagate (energia, CO2, costo) dai task posizionati finora
	// (correzione del doppio conteggio, 21/11).
	shipped := make(map[string]bool)

	// [NUOVO 22/11, punto 27 — join dopo un ChoiceTask] Un task si pianifica
	// solo quando TUTTI i suoi predecessori nel grafo sono a posto, non solo
	// quelli raggiunti da questa visita. Per ogni predecessore non raggiunto:
	//   - già eseguito, o già visitato da una pianificazione precedente
	//     (known): il suo output è sull'origine (il nodo che lo ha prodotto lo
	//     ha restituito a fine segmento, perché il task non era ancora
	//     eseguibile) e arriva insieme alla richiesta di offloading; entra tra
	//     gli input come "earlier". Il suo tempo non ritarda il task (è già
	//     passato o scorre in parallelo);
	//   - scartato da un ChoiceTask (Skipped): nessun dato;
	//   - altrimenti è dietro un ChoiceTask non ancora eseguito: il task è
	//     BLOCCATO e resta fuori dal piano, con tutto ciò che lo segue, fino
	//     alla ripianificazione dopo quel ChoiceTask.
	// Prima un join con un ramo dietro un ChoiceTask veniva pianificato già al
	// primo piano, come se dipendesse solo dagli altri rami (inizio e dati
	// stimati per difetto), e la ripianificazione non lo correggeva più.
	isStart := make(map[TaskId]bool, len(ready))
	for _, tid := range ready {
		isStart[tid] = true
	}
	blocked := make(map[TaskId]bool)
	for tid := range reached {
		if isStart[tid] {
			continue
		}
		for _, prev := range r.W.GetPreviousTasks(tid) {
			if reached[prev] {
				continue // contato in pending: si aspetta che venga pianificato
			}
			switch {
			case p.Status[prev] == Skipped:
			case p.Status[prev] == Executed, known[prev]:
				// Prodotto prima di questa pianificazione: il suo output è già
				// (o sarà, a fine del suo segmento) sull'origine e arriva al
				// nodo del task insieme alla richiesta di offloading.
				bytes := taskOutputBytes(r.W, prev)
				if est, placed := knownPlacement[prev]; placed {
					bytes = outputBytes(est.functionName, taskFunctionName(r.W, prev))
				}
				inputsOf[tid] = append(inputsOf[tid], elDataSource{loc: origin, bytes: bytes, producer: prev, item: prev, earlier: true})
			default:
				blocked[tid] = true
			}
		}
	}

	visited := make(map[TaskId]bool)
	for len(ready) > 0 {
		tid := ready[0]
		ready = ready[1:]
		if visited[tid] {
			continue
		}
		visited[tid] = true

		task := r.W.Tasks[tid]
		finish := startAt[tid]
		// outputs: dove si troverà l'output di questo task, passato ai
		// successori. Default (task strutturali come Start, Pass,
		// ParallelTask, ma anche un FunctionTask rimasto fuori dal piano):
		// girano sull'ORIGINE, perché non entrano mai nel piano di un nodo
		// remoto (punto 23), quindi i loro dati passano dall'origine
		// (viaOrigin), anche se provenivano da un nodo remoto.
		outputs := viaOriginSources(inputsOf[tid])

		if ft, isFunc := task.(*FunctionTask); isFunc {
			// [punto 28] Capacità di ogni candidato all'istante di inizio
			// stimato del task: stato letto meno le prenotazioni attive in
			// quell'istante (rami paralleli e altre richieste in corso).
			taskStart := planTime.Add(time.Duration(startAt[tid] * float64(time.Second)))
			taskCandidates := policy.candidatesAt(candidates, taskStart)
			if est := policy.placeFunctionTask(r, tid, ft, taskCandidates, origin, inputsOf[tid], shipped, residualDeadline, startAt[tid], minAccuracy, accChain, &deadlineAtRisk); est != nil {
				placement[tid] = est
				policy.reserve(r.Id, tid, est, taskStart)
				finish += est.completionTime.Seconds()
				accChain *= est.accuracy
				outputs = []elDataSource{{loc: est.candidate, bytes: outputBytes(est.functionName, ft.Func), producer: tid}}
			} else {
				outputs = []elDataSource{{loc: origin, bytes: outputBytes(ft.Func, ft.Func), viaOrigin: true, producer: tid}}
			}
		}
		if finish > makespan {
			makespan = finish
		}

		// Propagazione ai successori (topologica): inizio = massimo delle
		// fine dei predecessori, input = unione degli output dei
		// predecessori; il successore entra nella visita quando sono stati
		// visitati tutti i suoi predecessori.
		for _, s := range planSuccessors(task) {
			if finish > startAt[s] {
				startAt[s] = finish
			}
			for _, out := range outputs {
				out.item = tid // il motore invia i dati indicizzati per predecessore immediato
				inputsOf[s] = append(inputsOf[s], out)
			}
			pending[s]--
			if pending[s] == 0 && !blocked[s] {
				ready = append(ready, s)
			}
		}
	}

	logPlan(r.Id, placement, makespan, residualDeadline)
	log.Printf("EnergyLess: richiesta %s, prenotazioni attive dopo il piano: %s", r.Id, policy.reservationsSummary())
	return placement, visited
}

// placeFunctionTask sceglie la coppia nodo-funzione/variante per un
// FunctionTask (righe 16-23 dell'Alg. 1, esteso con varianti, deadline
// residua, soglia di accuratezza e trasferimenti dati). Ritorna nil se il
// task resta fuori dal piano (funzione non trovata o nessuna coppia
// fattibile): in quel caso girerà sull'origine.
func (policy *EnergyLessPolicy) placeFunctionTask(r *Request, tid TaskId, ft *FunctionTask, candidates []elCandidate, origin elCandidate, inputs []elDataSource, shipped map[string]bool, residualDeadline, start, minAccuracy, accChain float64, deadlineAtRisk *bool) *elEstimate {
	f, found := function.GetFunction(ft.Func)
	if !found {
		log.Printf("EnergyLess: funzione %s del task %s non trovata, task lasciato fuori dal piano", ft.Func, tid)
		return nil
	}
	flow := elDataFlow{
		origin:         origin,
		inputs:         inputs,
		returnToOrigin: isLastFunctionTask(r.W, ft),
		baseFunc:       f.Name,
		shipped:        shipped,
	}

	// Budget residuo per QUESTO task (punto 13, esteso col punto 24): la
	// deadline della richiesta meno l'istante di inizio stimato del task
	// (fine del predecessore più lento).
	remainingDeadline := residualDeadline
	if !math.IsInf(residualDeadline, 1) {
		remainingDeadline = residualDeadline - start
	}

	// Accuratezza minima richiesta a QUESTO task: da accChain * acc >=
	// minAccuracy segue acc >= minAccuracy / accChain (<= 1 per invariante).
	minTaskAccuracy := minAccuracy / accChain
	best, err := policy.selectBest(f, candidates, flow, remainingDeadline, minTaskAccuracy)
	if err != nil {
		// [DECISO 18/11 con il prof. Russo Russo, punto 21] Nessuna coppia
		// rispetta la deadline residua: non si ripiega sulla coppia più
		// veloce, si toglie solo il vincolo di deadline e si prende la coppia
		// che la funzione obiettivo sceglierebbe comunque con gli stessi pesi
		// (capacità, banda e accuratezza restano vincoli).
		best, err = policy.selectBest(f, candidates, flow, math.Inf(1), minTaskAccuracy)
		if err != nil {
			log.Printf("EnergyLess: nessuna coppia fattibile per %s (task %s), nemmeno senza il vincolo di deadline: %v", ft.Func, tid, err)
			return nil
		}
		if !*deadlineAtRisk {
			*deadlineAtRisk = true
			log.Printf("EnergyLess: richiesta %s, il piano prevede di sforare la deadline al task %s (budget residuo %.3fs): piano mantenuto, %s su %s (%.3fs)",
				r.Id, tid, remainingDeadline, best.functionName, best.candidate.key, best.completionTime.Seconds())
		}
	}
	if best.functionName != f.Name {
		// Ha vinto una variante: la scelta resta in placement[tid].functionName
		// e viene resa esecutiva da variantChoices e FunctionTask.exec()
		// (punto 16).
		log.Printf("EnergyLess: per il task %s scelta la variante %s al posto di %s (accuratezza task %.3f, di catena %.3f, soglia %.3f)",
			tid, best.functionName, f.Name, best.accuracy, accChain*best.accuracy, minAccuracy)
	}
	// Registra le tratte usate dalla coppia scelta: i task posizionati dopo
	// non ne pagheranno di nuovo energia, CO2 e costo (doppio conteggio).
	policy.transferEstimate(flow, best.candidate, outputBytes(best.functionName, f.Name), shipped)
	return best
}

// planSuccessors sono i task da visitare dopo t nella pianificazione
// statica (punto 24): il successore di un task unario, i rami di un
// FanOutTask; nessuno per un ConditionalTask (il ramo si conosce solo a
// runtime: lo si pianifica dopo l'esecuzione del ChoiceTask, punto 27) e per
// la fine del workflow.
func planSuccessors(t Task) []TaskId {
	switch tt := t.(type) {
	case UnaryTask:
		if next := tt.GetNext(); next != "" {
			return []TaskId{next}
		}
	case FanOutTask:
		return tt.GetBranches()
	}
	return nil
}

// viaOriginSources marca gli input di un task che gira sull'origine come
// "passati dall'origine": il successore dovrà riceverli dall'origine anche
// se erano stati prodotti sul suo stesso nodo (punto 23).
func viaOriginSources(in []elDataSource) []elDataSource {
	out := make([]elDataSource, len(in))
	for i, s := range in {
		s.viaOrigin = true
		out[i] = s
	}
	return out
}

// logPlan scrive il piano calcolato in una riga di log (una volta per piano):
// per ogni task il nodo, la funzione o variante scelta e il tempo stimato, più
// il tempo stimato della catena rispetto alla deadline residua. Serve a
// verificare il piano negli esperimenti, dato che dai report dei task si vede
// solo dove è stato eseguito ciascun task, non cosa aveva deciso la policy.
func logPlan(reqId string, placement map[TaskId]*elEstimate, chainSec float64, residualDeadline float64) {
	ids := make([]TaskId, 0, len(placement))
	for tid := range placement {
		ids = append(ids, tid)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	parts := make([]string, 0, len(ids))
	for _, tid := range ids {
		est := placement[tid]
		parts = append(parts, fmt.Sprintf("%s->%s/%s %.3fs[%s] %.1fJ", tid, est.candidate.key, est.functionName, est.completionTime.Seconds(), est.timeSource, est.energyJoules))
	}
	deadline := "nessuna"
	if !math.IsInf(residualDeadline, 1) {
		deadline = fmt.Sprintf("%.3fs", residualDeadline)
	}
	log.Printf("EnergyLess: piano per la richiesta %s (%d task, catena stimata %.3fs, deadline residua %s): %s",
		reqId, len(ids), chainSec, deadline, strings.Join(parts, ", "))
}

// computeDecisionsFromPlacement traduce il placement già calcolato (in
// cache) nelle OffloadingDecision del round corrente, senza richiamare
// selectBest(): legge solo quanto già deciso in buildPlan().
func (policy *EnergyLessPolicy) computeDecisionsFromPlacement(p *Progress, placement map[TaskId]*elEstimate, variants map[TaskId]string) []OffloadingDecision {
	// [NUOVO 21/11, punto 24] Una decisione per ogni nodo remoto che ha
	// almeno un task PRONTO nel piano. Con il fork-join più task possono
	// essere pronti nello stesso round (i rami di un ParallelTask): il motore
	// del main (Invoke) accetta più decisioni e, nello stesso round, esegue in
	// locale i task pronti non offloadati. Sostituisce la logica di
	// ILP/HEFTless (un solo nodo remoto per round, e nessun offloading se un
	// task pronto è assegnato al nodo locale), che con task paralleli
	// serializzava i rami. Sulle catene (un task pronto per round) il
	// comportamento non cambia. I task pronti fuori dal piano (es. dopo un
	// ChoiceTask) restano in locale.
	remotes := make(map[string]elCandidate)
	for _, tid := range p.ReadyToExecute {
		if est, ok := placement[tid]; ok && !est.candidate.isLocal {
			remotes[est.candidate.key] = est.candidate
		}
	}
	if len(remotes) == 0 {
		return []OffloadingDecision{{Offload: false}}
	}
	keys := make([]string, 0, len(remotes))
	for k := range remotes {
		keys = append(keys, k)
	}
	sort.Strings(keys) // ordine deterministico, utile per log ed esperimenti riproducibili

	decisions := make([]OffloadingDecision, 0, len(keys))
	for _, key := range keys {
		// Visione totale (punto 11, ricevimento del 02/10): il piano inviato a
		// un nodo contiene TUTTI i task del placement assegnati a quel nodo,
		// non solo quelli pronti ora. Il nodo remoto esegue quelli con i dati
		// disponibili e restituisce il controllo all'origine davanti a un task
		// non suo o non ancora eseguibile (per esempio un join con un ramo su
		// un altro nodo). Il motore invia comunque solo i task pronti
		// (dispatchReadyTasks filtra ToExecute su ReadyToExecute).
		toExecute := make([]TaskId, 0, len(placement))
		for tid, est := range placement {
			if est.candidate.key == key {
				toExecute = append(toExecute, tid)
			}
		}
		sort.Slice(toExecute, func(i, j int) bool { return toExecute[i] < toExecute[j] })

		// Le varianti scelte per i task offloadati viaggiano nel piano
		// (punto 16): il nodo remoto non ripianifica.
		var planVariants map[TaskId]string
		for _, tid := range toExecute {
			if v, chosen := variants[tid]; chosen {
				if planVariants == nil {
					planVariants = make(map[TaskId]string)
				}
				planVariants[tid] = v
			}
		}

		decisions = append(decisions, OffloadingDecision{
			Offload:    true,
			RemoteHost: remotes[key].apiURL,
			OffloadingPlan: OffloadingPlan{
				ToExecute: toExecute,
				Variants:  planVariants,
			},
		})
	}
	return decisions
}

// cacheSolution e getCachedPlan implementano la cache del piano per
// richiesta (punto 6). Non più una reimplementazione "a modello" come nella
// versione precedente di questo file: ora che ho il codice reale di
// cacheSolution/getCachedSolution in remote_offloading_policy.go (ILP/
// HEFTless), questa è una copia fedele della stessa logica — valori
// puntatore nella mappa (come placementCache) e ttl come contatore di
// letture, non come scadenza temporale (vedi energyLessCacheEntry). Resta
// una copia locale, non una funzione condivisa: il value type della mappa è
// diverso (map[TaskId]*elEstimate qui, taskPlacement=map[TaskId]string
// nell'originale), quindi un refactor verso un helper generico condiviso
// resta possibile ma non fatto qui — vedi "Domande ancora aperte".
func (policy *EnergyLessPolicy) cacheSolution(requestId string, placement map[TaskId]*elEstimate, visited map[TaskId]bool) {
	policy.cacheMu.Lock()
	defer policy.cacheMu.Unlock()
	policy.cache[requestId] = &energyLessCacheEntry{
		placement: placement,
		ttl:       policy.cacheTTLInit,
		visited:   visited,
	}
}

// ---------------------------------------------------------------------
// Tabella delle risorse prenotate (punto 28).
// ---------------------------------------------------------------------

// elReservation è una prenotazione di risorse su un nodo edge per un task
// pianificato: memoria e CPU della funzione scelta, nell'intervallo stimato
// [start, end) in tempo assoluto.
type elReservation struct {
	requestId  string
	task       TaskId
	node       string
	start, end time.Time
	memMB, cpu float64
}

// reserve registra la prenotazione del task appena posizionato. Il cloud non
// si prenota (capacità infinita, deciso il 02/10). Va chiamata con planMu.
func (policy *EnergyLessPolicy) reserve(requestId string, tid TaskId, est *elEstimate, start time.Time) {
	if est.candidate.key == "CLOUD" {
		return
	}
	mem, cpu := 0.0, 1.0
	if fn, ok := function.GetFunction(est.functionName); ok {
		mem, cpu = float64(fn.MemoryMB), fn.CPUDemand
	}
	policy.resMu.Lock()
	defer policy.resMu.Unlock()
	policy.reservations = append(policy.reservations, elReservation{
		requestId: requestId, task: tid, node: est.candidate.key,
		start: start, end: start.Add(est.completionTime), memMB: mem, cpu: cpu,
	})
}

// candidatesAt restituisce una copia dei candidati con memoria e CPU
// disponibili all'istante at: stato letto meno le prenotazioni attive in
// quell'istante (start <= at < end). Un task in catena parte quando finisce
// il precedente, quindi non vede la prenotazione del precedente (riuso dello
// stesso core); i rami paralleli e le richieste concorrenti invece si sommano.
func (policy *EnergyLessPolicy) candidatesAt(candidates []elCandidate, at time.Time) []elCandidate {
	policy.resMu.Lock()
	defer policy.resMu.Unlock()
	out := append([]elCandidate(nil), candidates...)
	for i := range out {
		if out[i].key == "CLOUD" {
			continue
		}
		for _, res := range policy.reservations {
			if res.node == out[i].key && !at.Before(res.start) && at.Before(res.end) {
				out[i].availMemMB -= res.memMB
				out[i].availCPU -= res.cpu
			}
		}
	}
	return out
}

// pruneReservations toglie le prenotazioni che lo stato dei nodi riflette già
// o che non servono più: scadute (end <= now); sul nodo locale, già iniziate
// (lo stato locale è letto in tempo reale dai container attivi); su un vicino,
// iniziate prima del suo ultimo heartbeat (lo stato ricevuto le include, o il
// task è già finito). Restano quelle future, che nessuno stato può ancora
// mostrare. È il "reset a ogni heartbeat" deciso il 18/11, senza perdere le
// prenotazioni non ancora visibili. Va chiamata con planMu.
func (policy *EnergyLessPolicy) pruneReservations(now time.Time) {
	local := ""
	if registration.SelfRegistration != nil {
		local = registration.SelfRegistration.Key
	}
	heartbeat := make(map[string]int64)
	for key, status := range registration.GetFullNeighborInfo() {
		if status != nil {
			heartbeat[key] = status.LastUpdateTime
		}
	}
	policy.resMu.Lock()
	defer policy.resMu.Unlock()
	kept := policy.reservations[:0]
	for _, res := range policy.reservations {
		switch {
		case !res.end.After(now):
		case res.node == local && !res.start.After(now):
		case heartbeat[res.node] > 0 && res.start.Unix() <= heartbeat[res.node]:
		default:
			kept = append(kept, res)
		}
	}
	policy.reservations = kept
}

// releaseReservations toglie tutte le prenotazioni di una richiesta terminata.
func (policy *EnergyLessPolicy) releaseReservations(requestId string) {
	policy.resMu.Lock()
	defer policy.resMu.Unlock()
	kept := policy.reservations[:0]
	for _, res := range policy.reservations {
		if res.requestId != requestId {
			kept = append(kept, res)
		}
	}
	policy.reservations = kept
}

// reservationsSummary riassume le prenotazioni per nodo, per il log: numero
// di task prenotati e risorse prenotate ADESSO (prenotazioni attive in questo
// istante; quelle future di una catena non si sommano).
func (policy *EnergyLessPolicy) reservationsSummary() string {
	policy.resMu.Lock()
	defer policy.resMu.Unlock()
	if len(policy.reservations) == 0 {
		return "nessuna"
	}
	type agg struct {
		n        int
		mem, cpu float64
	}
	now := time.Now()
	byNode := make(map[string]*agg)
	var nodes []string
	for _, res := range policy.reservations {
		a, ok := byNode[res.node]
		if !ok {
			a = &agg{}
			byNode[res.node] = a
			nodes = append(nodes, res.node)
		}
		a.n++
		if !now.Before(res.start) && now.Before(res.end) {
			a.mem += res.memMB
			a.cpu += res.cpu
		}
	}
	sort.Strings(nodes)
	parts := make([]string, 0, len(nodes))
	for _, n := range nodes {
		a := byNode[n]
		parts = append(parts, fmt.Sprintf("%s: %d task prenotati, ora %.0f MB e %.1f CPU", n, a.n, a.mem, a.cpu))
	}
	return strings.Join(parts, "; ")
}

// Controllo a tempo di compilazione: la policy implementa l'interfaccia.
var _ RequestCompletionListener = (*EnergyLessPolicy)(nil)

// RequestCompleted implementa RequestCompletionListener (offloading_policy.go)
// ed è chiamata quando una richiesta termina sul nodo di origine, con
// successo o con errore (err != nil).
//
// [NUOVO 21/11, punto 25] Elimina la voce della richiesta dalla cache dei
// piani: prima restava in memoria per sempre (TTL di 9999 letture, che una
// richiesta non consuma mai).
//
// [NUOVO 22/11, punto 26 — deciso il 18/11 con il prof. Russo Russo]
// Aggiorna i due contatori delle richieste: completate (workflow terminato
// senza errori) e completate rispettando anche la deadline (tempo di
// risposta <= QoS.MaxRespT, misurato da r.Arrival come fa l'API). Una
// richiesta senza deadline (MaxRespT <= 0) conta come rispettata. Le
// richieste terminate con errore non entrano in nessuno dei due e sono
// contate a parte.
func (policy *EnergyLessPolicy) RequestCompleted(r *Request, err error) {
	policy.cacheMu.Lock()
	delete(policy.cache, r.Id)
	remaining := len(policy.cache)
	policy.cacheMu.Unlock()
	policy.releaseReservations(r.Id) // [punto 28] la richiesta non occupa più risorse

	respT := time.Since(r.Arrival).Seconds()
	deadline := r.QoS.MaxRespT
	outcome := ""
	switch {
	case err != nil:
		policy.stats.failed.Add(1)
		outcome = fmt.Sprintf("terminata con errore (%v)", err)
	case deadline <= 0:
		policy.stats.completed.Add(1)
		policy.stats.withinDeadline.Add(1)
		outcome = fmt.Sprintf("completata in %.3fs (nessuna deadline)", respT)
	case respT <= deadline:
		policy.stats.completed.Add(1)
		policy.stats.withinDeadline.Add(1)
		outcome = fmt.Sprintf("completata in %.3fs, deadline %.3fs rispettata", respT, deadline)
	default:
		policy.stats.completed.Add(1)
		outcome = fmt.Sprintf("completata in %.3fs, deadline %.3fs NON rispettata", respT, deadline)
	}
	completed, within, failed := policy.Stats()
	pct := 0.0
	if completed > 0 {
		pct = 100 * float64(within) / float64(completed)
	}
	log.Printf("EnergyLess: richiesta %s %s | contatori: completate %d, entro la deadline %d (%.1f%%), con errore %d | piani in cache: %d",
		r.Id, outcome, completed, within, pct, failed, remaining)
}

// Stats restituisce i contatori delle richieste dall'avvio del nodo (punto
// 26): completate, completate entro la deadline, terminate con errore.
func (policy *EnergyLessPolicy) Stats() (completed, withinDeadline, failed uint64) {
	return policy.stats.completed.Load(), policy.stats.withinDeadline.Load(), policy.stats.failed.Load()
}

func (policy *EnergyLessPolicy) getCachedPlan(requestId string) (map[TaskId]*elEstimate, map[TaskId]bool, bool) {
	policy.cacheMu.Lock()
	defer policy.cacheMu.Unlock()

	entry, found := policy.cache[requestId]
	if !found {
		return nil, nil, false
	}

	// Stesso comportamento di getCachedSolution: la entry si consuma una
	// lettura per volta; quando il contatore arriva a 0 viene eliminata.
	if entry.ttl > 0 {
		entry.ttl--
		return entry.placement, entry.visited, true
	}

	delete(policy.cache, requestId)
	return nil, nil, false
}

// unvisitedReadyTasks sono i task pronti e ancora da eseguire che la visita
// di buildPlan() non ha mai raggiunto: il primo task del ramo scelto da un
// ChoiceTask eseguito dopo la pianificazione (punto 27).
func unvisitedReadyTasks(p *Progress, visited map[TaskId]bool) []TaskId {
	var fresh []TaskId
	for _, tid := range p.ReadyToExecute {
		if !visited[tid] && p.Status[tid] == Pending {
			fresh = append(fresh, tid)
		}
	}
	return fresh
}

// chosenAccuracy è il prodotto delle accuratezze già scelte nel piano per i
// task che non sono stati scartati da un ChoiceTask (Skipped): il punto di
// partenza del vincolo di accuratezza in una ripianificazione (punto 27).
func chosenAccuracy(p *Progress, placement map[TaskId]*elEstimate) float64 {
	acc := 1.0
	for tid, est := range placement {
		if p.Status[tid] != Skipped {
			acc *= est.accuracy
		}
	}
	return acc
}

// mergePlans aggiunge al piano esistente i task di una ripianificazione,
// senza sostituire quelli già presenti (punto 27, coerente col punto 21).
// Restituisce mappe nuove: quelle in cache non vengono modificate.
func mergePlans(placement map[TaskId]*elEstimate, visited map[TaskId]bool, added map[TaskId]*elEstimate, addedVisited map[TaskId]bool) (map[TaskId]*elEstimate, map[TaskId]bool) {
	mp := make(map[TaskId]*elEstimate, len(placement)+len(added))
	for tid, est := range placement {
		mp[tid] = est
	}
	for tid, est := range added {
		if _, exists := mp[tid]; !exists {
			mp[tid] = est
		}
	}
	mv := make(map[TaskId]bool, len(visited)+len(addedVisited))
	for tid := range visited {
		mv[tid] = true
	}
	for tid := range addedVisited {
		mv[tid] = true
	}
	return mp, mv
}

// rankReadyTasks ordina i task pronti per rank decrescente (tempo medio
// stimato più alto = priorità più alta, stile HEFT/upward-rank).
// Ranking dei task pronti (righe 6-12, Alg. 1): r_f = tempo medio
// stimato sulle istanze candidate.
func (policy *EnergyLessPolicy) rankReadyTasks(r *Request, tasks []TaskId, candidates []elCandidate) []TaskId {
	type ranked struct {
		id   TaskId
		rank float64
	}
	rm := metrics.GetMetrics()
	ranks := make([]ranked, 0, len(tasks))

	for _, tid := range tasks {
		ft, isFunc := r.W.Tasks[tid].(*FunctionTask)
		if !isFunc {
			ranks = append(ranks, ranked{id: tid, rank: 0})
			continue
		}
		var sum float64
		for _, c := range candidates {
			t := estimateExecTime(rm, c, ft.Func)
			if t <= 0 {
				if tv, ok := policy.tuning.lookup(c, ft.Func); ok && tv.execTime > 0 {
					t = tv.execTime // misura del tuning da file (punto 20)
				} else {
					t = policy.fallbackExecTime() // nessuna metrica reale: default.exec.time (punto 18)
				}
			}
			sum += t
		}
		avg := 0.0
		if len(candidates) > 0 {
			avg = sum / float64(len(candidates))
		}
		ranks = append(ranks, ranked{id: tid, rank: avg})
	}

	for i := 1; i < len(ranks); i++ {
		for j := i; j > 0 && ranks[j].rank > ranks[j-1].rank; j-- {
			ranks[j], ranks[j-1] = ranks[j-1], ranks[j]
		}
	}

	out := make([]TaskId, len(ranks))
	for i, rk := range ranks {
		out[i] = rk.id
	}
	return out
}

// gatherCandidates raccoglie le istanze candidate: se stesso (locale), i vicini edge
// con capacità disponibile (stesso filtro di prepareParameters() in
// remote_offloading_policy.go), e l'eventuale target cloud configurato.
func (policy *EnergyLessPolicy) gatherCandidates() []elCandidate {
	candidates := make([]elCandidate, 0, 4)

	local := registration.SelfRegistration
	if local != nil {
		candidates = append(candidates, elCandidate{
			key:              local.Key,
			isLocal:          true,
			area:             local.Area,
			availMemMB:       float64(node.LocalResources.AvailableMemory()),
			statusTime:       time.Now().Unix(),
			availCPU:         node.LocalResources.AvailableCPUs(),
			co2Intensity:     node.LocalResources.CO2Intensity(),
			procPowerW:       node.LocalResources.ProcessingPower(),
			cores:            node.LocalResources.TotalCPUs(),
			idlePowerW:       node.LocalResources.IdlePower(),
			txEnergyJPerByte: node.LocalResources.TxEnergyPerByte(),
			rxEnergyJPerByte: node.LocalResources.RxEnergyPerByte(),
		})
	}

	// registration.GetFullNeighborInfo() restituisce una mappa key → *StatusInformation per ogni
	// vicino conosciuto. Questo StatusInformation viene propagato via heartbeat UDP periodico: ogni
	// vicino manda periodicamente il proprio stato (memoria, CPU, CO2Intensity, ProcessingPowerConsumption,
	// TxEnergyConsumption, RxEnergyConsumption).
	for key, status := range registration.GetFullNeighborInfo() {
		if status == nil || status.AvailableMemory <= 0 || (status.TotalCPU-status.UsedCPU) <= 0 { // scarta i vicini senza risorse libere
			continue
		}
		peer := registration.GetPeerFromKey(key) // recupera l'apiURL
		if peer == nil {
			continue
		}
		candidates = append(candidates, elCandidate{
			key:              key,
			apiURL:           peer.APIUrl(),
			area:             peer.Area,
			availMemMB:       float64(status.AvailableMemory),
			statusTime:       status.LastUpdateTime,
			availCPU:         status.TotalCPU - status.UsedCPU,
			co2Intensity:     status.CO2Intensity,
			procPowerW:       status.ProcessingPowerConsumption,
			cores:            status.TotalCPU,
			idlePowerW:       status.IdlePowerConsumption,
			txEnergyJPerByte: status.TxEnergyConsumption,
			rxEnergyJPerByte: status.RxEnergyConsumption,
		})
	}

	if cloud := registration.GetRemoteOffloadingTarget(); cloud != nil {
		// Deciso 02/10: il Cloud si assume a risorse infinite — niente query
		// di stato per la CAPACITÀ, niente controllo di capacità (vedi
		// estimateOnCandidate, che salta il vincolo per key == "CLOUD").
		// availMemMB/availCPU restano quindi a zero e non vengono mai letti
		// per questo candidato — questo non cambia con la query sotto, che
		// riguarda solo la CO2Intensity, non le risorse.
		//
		// [RISOLTO punto 5 "Domande ancora aperte"] co2Intensity del cloud:
		// query live allo stesso endpoint "/status" del load balancer
		// cloud, con lo stesso pattern HTTP già usato in produzione da
		// discoverCloudCandidates()/fetchNodeStatus() in
		// internal/scheduling/co2_qos_policy.go (fetchNodeStatus non è
		// esportata da quel package, quindi qui è reimplementata localmente
		// in fetchCloudStatus(), stesso endpoint, stesso timeout 3s, stesso
		// tipo di risposta registration.StatusInformation). Confermato da
		// Cecilia (16/10): questo pattern è quello corretto, non serve
		// altro.
		//
		// Fallback se la query fallisce (timeout, cloud irraggiungibile,
		// status code non 200, JSON non decodificabile): si usa l'ULTIMO
		// valore live ottenuto con successo dal cloud (lastKnownCloudCO2),
		// non la CO2Intensity del nodo locale — Cecilia ha confermato
		// (16/10) che quel fallback "ingenuo" copiato da
		// discoverCloudCandidates() non è una scelta di design validata,
		// solo un aspetto mai sistemato in quel codice, e ha lasciato la
		// scelta a questa bozza. Il nodo locale come stand-in resta usato
		// SOLO al primissimo fallimento, quando non esiste ancora nessun
		// valore noto del cloud da riusare (caso limite: cloud irraggiungibile
		// fin dalla primissima richiesta gestita da questo nodo).
		cloudCO2 := 0.0
		if status, err := fetchCloudStatus(*cloud); err == nil && status != nil {
			cloudCO2 = status.CO2Intensity
			policy.cloudCO2Mu.Lock()
			policy.lastKnownCloudCO2 = cloudCO2
			policy.hasLastKnownCloudCO2 = true
			policy.cloudCO2Mu.Unlock()
		} else {
			policy.cloudCO2Mu.Lock()
			if policy.hasLastKnownCloudCO2 {
				cloudCO2 = policy.lastKnownCloudCO2
			} else {
				cloudCO2 = node.LocalResources.CO2Intensity() // stand-in solo alla primissima query fallita
			}
			policy.cloudCO2Mu.Unlock()
			log.Printf("energyless: query live CO2Intensity del cloud (%s) fallita: %v — uso %v come fallback", cloud.APIUrl(), err, cloudCO2)
		}
		c := elCandidate{
			key:          "CLOUD",
			apiURL:       cloud.APIUrl(),
			area:         cloud.Area,
			co2Intensity: cloudCO2,
			cores:        policy.cloudCores, // il cloud non manda TotalCPU: cloud.cores del file della policy (punto 22)
		}
		if policy.regionsCfg != nil {
			if info, ok := policy.regionsCfg.Area(cloud.Area); ok {
				c.procPowerW = info.ProcessingPowerConsumption
				c.idlePowerW = info.IdlePowerConsumption
				// Stessa unità di consumption.energy.tx/rx dei nodi (nJ/byte):
				// node.Resources la divide per 1e9, qui va fatto lo stesso
				// (punto 23).
				c.txEnergyJPerByte = info.TxEnergyConsumption / 1e9
				c.rxEnergyJPerByte = info.RxEnergyConsumption / 1e9
			}
		}
		candidates = append(candidates, c)
	}

	return candidates
}

// fetchCloudStatus interroga lo stato live del load balancer del target
// cloud configurato (registration.GetRemoteOffloadingTarget()), per
// ottenere una CO2Intensity reale e aggiornata invece di approssimarla con
// quella del nodo locale. Replica localmente, identico, il pattern HTTP già
// usato in produzione da fetchNodeStatus()/discoverCloudCandidates() in
// internal/scheduling/co2_qos_policy.go (non riusabile direttamente: è una
// funzione privata di quel package) — stesso endpoint "<APIUrl>/status",
// stesso timeout di 3s, stessa risposta decodificata in
// registration.StatusInformation (tipo esportato).
func fetchCloudStatus(lb registration.NodeRegistration) (*registration.StatusInformation, error) {
	client := http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(lb.APIUrl() + "/status")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New(resp.Status)
	}
	var status registration.StatusInformation
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, err
	}
	return &status, nil
}

// validVariants ritorna le varianti valide di f (punto 1): quelle che
// superano ValidateVariantMetadata(). f stessa non deve essere già una
// variante (si valutano varianti solo a partire dalla funzione base).
//
// Verificato sul vero internal/function/function.go condiviso nel progetto
// (package function):
//   - GetVariantsOf(baseName string) ([]*Function, error): ritorna solo le
//     funzioni già filtrate con IsVariant() && DefaultFunction == baseName
//     (vedi filterVariants), ordinate per nome — da qui la necessità di
//     gestire anche l'errore, non solo il value, come corretto sotto.
//   - ValidateVariantMetadata() esiste davvero come metodo su *Function
//     (controlla: non-default con DefaultFunction vuoto, nome diverso dal
//     default, SpeedUp > 0, Utility in [0,1]) — il precedente "Unresolved
//     reference" segnalato non dipendeva quindi da un'API
//     inesistente, ma da una versione di function.go non ancora aggiornata
//     nel suo IDE/branch locale rispetto a questo file. Richiamata di
//     nuovo qui come in origine.
func validVariants(f *function.Function) []*function.Function {
	if f.IsVariant() {
		return nil
	}
	variants, err := function.GetVariantsOf(f.Name)
	if err != nil {
		log.Printf("EnergyLess: errore nel recupero delle varianti di %s: %v", f.Name, err)
		return nil
	}
	var valid []*function.Function
	for _, v := range variants {
		if v == nil {
			continue
		}
		if err := v.ValidateVariantMetadata(); err != nil {
			continue
		}
		valid = append(valid, v)
	}
	return valid
}

// selectBest è l'equivalente locale delle righe 16-23 dell'Alg. 1, esteso
// per il punto 1 (varianti al posto della decomposizione): per la funzione
// f, itera su tutti i candidati (= coppie (i,j) del paper, con j collassato
// perché in Serverledge lo storage è unico) e, per ciascuno, valuta sia f
// sia ogni sua variante valida, chiamando l'equivalente di EST() per
// stimare le 4 metriche. Normalizza sull'obiettivo massimo osservato tra
// TUTTE le combinazioni (candidato, funzione-o-variante) valutate per
// QUESTO task, e sceglie il minimo entro la deadline residua.
//
// [NUOVO 30/10, ricevimento del 22/10] minTaskAccuracy è l'accuratezza minima
// che QUESTO task deve avere perché il prodotto di catena resti sopra la
// soglia della richiesta (calcolata da buildPlan, vedi accChain). La funzione
// f del task ha per definizione accuratezza 1.0 (è quella che l'utente ha
// scelto: anche se f fosse a sua volta registrata come variante, non viene
// degradata); una variante sostitutiva v ha accuratezza v.Utility ed è
// scartata PRIMA della stima se sta sotto minTaskAccuracy — così non entra
// nemmeno nella normalizzazione dell'Eq. 22, esattamente come una coppia
// infattibile per deadline o capacità.
func (policy *EnergyLessPolicy) selectBest(f *function.Function, candidates []elCandidate, flow elDataFlow, residualDeadline float64, minTaskAccuracy float64) (*elEstimate, error) {
	return policy.selectWithWeights(policy.weights, f, candidates, flow, residualDeadline, minTaskAccuracy)
}

// selectWithWeights contiene la logica di selectBest con i pesi dell'Eq. 22
// passati come parametro (usati solo per l'obiettivo, non per i vincoli).
func (policy *EnergyLessPolicy) selectWithWeights(weights energyLessWeights, f *function.Function, candidates []elCandidate, flow elDataFlow, residualDeadline float64, minTaskAccuracy float64) (*elEstimate, error) {
	rm := metrics.GetMetrics()
	var variants []*function.Function
	for _, v := range validVariants(f) {
		if v.Utility+accuracyEpsilon >= minTaskAccuracy {
			variants = append(variants, v)
		}
	}
	estimates := make([]elEstimate, 0, len(candidates)*(1+len(variants)))

	var maxTime, maxEnergy, maxCarbon, maxCost float64
	accumulate := func(e *elEstimate) {
		estimates = append(estimates, *e)
		maxTime = maxFloat(maxTime, e.completionTime.Seconds())
		maxEnergy = maxFloat(maxEnergy, e.energyJoules)
		maxCarbon = maxFloat(maxCarbon, e.carbonGrams)
		maxCost = maxFloat(maxCost, e.cost)
	}

	// iterazione su istanza i (= candidato)
	for _, c := range candidates {
		baseExecTime, baseEst, ok := policy.estimateOnCandidate(rm, c, f, f.Name, flow, residualDeadline, policy.fallbackExecTime())
		if ok {
			baseEst.accuracy = 1.0 // la funzione del task non è degradata
			accumulate(baseEst)
		}

		// Varianti della stessa funzione, sullo stesso candidato (punto 1).
		for _, v := range variants {
			fallback := policy.fallbackExecTime()
			// [CORRETTO 11/11, punto 18] Prima la condizione era "ok &&
			// SpeedUp > 0": se la funzione base non rientrava nella deadline
			// (ok=false), la variante senza metriche ricadeva sul ripiego
			// generico invece che su baseExecTime/SpeedUp, proprio nel caso
			// in cui una variante più leggera conta di più. estimateOnCandidate
			// ritorna execTime anche quando il candidato è scartato per la
			// deadline (è 0 solo se scartato per capacità).
			if baseExecTime > 0 && v.SpeedUp > 0 {
				// Approssimazione quando non esistono ancora metriche reali
				// per la variante su questo candidato: tempo della funzione
				// base scalato per lo SpeedUp dichiarato — stesso approccio
				// usato in internal/scheduling/co2_qos_optimizer.go
				// (populateOptimizerVariant) per lo stesso problema.
				fallback = baseExecTime / v.SpeedUp
			}
			_, vEst, vOk := policy.estimateOnCandidate(rm, c, v, v.Name, flow, residualDeadline, fallback)
			if !vOk {
				continue
			}
			vEst.accuracy = v.Utility
			accumulate(vEst)
			// NOTA: v.Utility agisce qui solo come VINCOLO (soglia minima,
			// filtro sopra). Come ulteriore termine pesato dell'obiettivo (Eq.
			// 22 ha 4 termini) è previsto dal ricevimento del 22/10 come
			// possibile estensione, non ancora fatta — vedi fondo file.
		}
	}

	if len(estimates) == 0 {
		return nil, fmt.Errorf("nessuna istanza candidata (funzione base o variante) soddisfa capacità/deadline")
	}

	var best *elEstimate
	for i := range estimates {
		e := &estimates[i]
		e.objective = weights.time*safeDiv(e.completionTime.Seconds(), maxTime) +
			weights.energy*safeDiv(e.energyJoules, maxEnergy) +
			weights.carbon*safeDiv(e.carbonGrams, maxCarbon) +
			weights.cost*safeDiv(e.cost, maxCost)
		if best == nil || e.objective < best.objective {
			best = e
		}
	}
	return best, nil
}

// estimateOnCandidate stima le 4 metriche (equivalente di EST, Alg. 2) per
// la coppia (fn, candidato). metricName è il nome usato per cercare le
// metriche Prometheus reali (f.Name per la funzione base, v.Name per una
// variante). fallbackExecTime è il tempo da usare quando non esiste ancora
// una metrica reale per (candidato, metricName): default.exec.time per la
// funzione base, baseExecTime/SpeedUp per una variante (vedi selectBest).
// Ritorna anche execTime (utile al chiamante per calcolare il fallback
// delle varianti) e ok=false se il candidato non è fattibile.
//
// [AGGIORNATO 17/10, punto 14 in testa al file] Prima di questa modifica
// mancavano, rispetto ad Alg. 2/Eq. 2/10/14/15 del paper: il cold start
// (λ_i^f) nel tempo di completamento, il costo di trasferimento (CT^f,
// Eq. 14) e di storage (CS^f, Eq. 15) nel costo, e un vero vincolo di banda
// (Eq. 20) — una banda non configurata per un candidato veniva trattata
// come "trasferimento istantaneo" invece che come candidato infattibile.
// Tutti e quattro aggiunti qui.
func (policy *EnergyLessPolicy) estimateOnCandidate(rm metrics.RetrievedMetrics, c elCandidate, fn *function.Function, metricName string, flow elDataFlow, residualDeadline float64, fallbackExecTime float64) (execTime float64, est *elEstimate, ok bool) {
	// Vincolo di capacità (Eq. 18-19 / ρ_i, Eq. 21 — chiarito 02/10 che ρ_i
	// corrisponde ad available CPU, già controllato qui): per il candidato
	// CLOUD il controllo è saltato interamente, perché si assume capacità
	// infinita (deciso 02/10, vedi gatherCandidates()).
	if c.key != "CLOUD" {
		if float64(fn.MemoryMB) > c.availMemMB || fn.CPUDemand > c.availCPU {
			return 0, nil, false
		}
	}

	// Priorità del tempo di esecuzione (21/11, resa visibile nel log):
	// 1) misura live di Prometheus per quel nodo e quella funzione (serve il
	//    Pushgateway: è lui ad aggiungere l'etichetta node alle metriche);
	// 2) misura del tuning salvata su file (punto 20);
	// 3) ripiego: base/SpeedUp per una variante, default.exec.time per la
	//    funzione base.
	timeSource := "misura"
	execTime = estimateExecTime(rm, c, metricName)
	tv, tuned := policy.tuning.lookup(c, metricName)
	if execTime <= 0 && tuned && tv.execTime > 0 {
		execTime = tv.execTime
		timeSource = "tuning"
	}
	if execTime <= 0 {
		execTime = fallbackExecTime
		timeSource = "speedup"
		if metricName == flow.baseFunc {
			timeSource = "default"
		}
	}
	if execTime <= 0 {
		execTime = policy.fallbackExecTime()
		timeSource = "default"
	}

	// Cold start (λ_i^f, Eq. 2): tempo di init ATTESO, già pesato per la
	// probabilità di cold start osservata — vedi estimateInitTime(). Prima
	// di questa modifica il piano ignorava del tutto questo termine,
	// sottostimando sistematicamente il completionTime di ogni candidato
	// (di più per i remoti, dove un container caldo è meno probabile che in
	// locale).
	initTime := estimateInitTime(rm, c, metricName, policy.fallbackInitTime(), tv, tuned)
	completion := time.Duration((execTime + initTime) * float64(time.Second))

	// [NUOVO 20/11, punto 23] Trasferimenti dati nodo a nodo: tempo (Eq. 3),
	// energia di rete (parte dati dell'Eq. 7), CO2 dei dati (Eq. 10) e costo
	// di trasferimento (CT^f, Eq. 14). Zero se l'input è già su questo nodo e
	// il risultato non deve tornare all'origine. Una tratta senza banda
	// configurata rende il candidato infattibile (vincolo di banda, Eq. 20).
	tr, feasible := policy.transferEstimate(flow, c, outputBytes(metricName, flow.baseFunc), nil)
	if !feasible {
		return execTime, nil, false
	}
	completion += time.Duration(tr.seconds * float64(time.Second))

	if !math.IsInf(residualDeadline, 1) && completion.Seconds() > residualDeadline {
		return execTime, nil, false // vincolo di deadline, Eq. 5
	}

	energyJ := energyJoules(c, execTime) + tr.energyJ
	// [RISOLTO nel ricevimento del 22/10] Eq. 10 del paper distingue
	// l'intensità di carbonio del CANDIDATO (ξ_i, per la componente di
	// esecuzione) da quella dello STORAGE (ξ_j, per la componente dati).
	// Confermato dal gruppo: rispetto all'equazione originale di
	// EnergyLess, che assume uno storage esterno selezionabile per nodo
	// (j ∈ S), in Serverledge si farà riferimento a etcd/al modello di
	// storage centralizzato — non serve modellare una selezione di storage
	// separati né quindi una CO2Intensity distinta per lo storage. Si
	// continua quindi a usare ξ_i anche per la componente dati (energyJ
	// include già sia compute che transfer, vedi energyJoules): non è più
	// un'approssimazione provvisoria in attesa di un dato mancante, è il
	// comportamento corretto per questa architettura.
	//
	// [DECISO 15/11, punto 19] Come nell'Eq. 10 del paper, la CO2 si calcola
	// solo dall'energia di ESECUZIONE e dei DATI (CO^f = ξ_i·E_exec + ξ_j·E_data;
	// anche l'Alg. 2, riga 4, non ha ε_i). Dal 20/11 (punto 22) anche
	// energyJ è senza idle, quindi la CO2 si calcola direttamente su energyJ.
	//
	// [20/11, punto 23] L'energia di esecuzione usa la CO2Intensity del
	// candidato; quella dei dati è già convertita in grammi tratta per tratta
	// (trasmissione con l'intensità del mittente, ricezione con quella del
	// destinatario), al posto dell'approssimazione ξ_j ≈ ξ_i.
	carbonG := (energyJoules(c, execTime)/3.6e6)*c.co2Intensity + tr.carbonG // Eq. 10, J->kWh come emissions.joulesToKWh
	cost := regionCost(c.area)*execTime + tr.cost                            // Eq. 13 (ΔC_i · ψ_i^f) + Eq. 14; niente CS^f (punto 23)

	return execTime, &elEstimate{
		candidate:      c,
		functionName:   metricName,
		completionTime: completion,
		energyJoules:   energyJ,
		carbonGrams:    carbonG,
		cost:           cost,
		timeSource:     timeSource,
	}, true
}

// estimateInitTime stima λ_i^f (Eq. 2): il contributo ATTESO del cold start
// alla preparazione del task, cioè tempo medio di init osservato * probabilità
// di cold start — stesso approccio di prepareParameters() in
// remote_offloading_policy.go (ILP/HEFTless), replicato qui fedelmente
// INCLUSA una piccola asimmetria già presente in quel codice: per il CLOUD,
// una probabilità mancante nella mappa (RemoteColdStartProbability) vale 0.0
// (il valore di default di una map[string]float64 in Go), cioè "nessun cold
// start atteso"; per un nodo edge/locale invece manca lo stesso default
// implicito e il codice ne mette uno esplicito a 1.0 ("sempre cold") quando
// il dato non c'è. Non è una scelta di design fatta qui: è così anche nel
// vero remote_offloading_policy.go, replicata per restare fedeli al
// comportamento originale (segnalabile a Cecilia come possibile incoerenza,
// ma non è stata introdotta da questa bozza).
func estimateInitTime(rm metrics.RetrievedMetrics, c elCandidate, funcName string, fallbackInit float64, tv tunedValues, tuned bool) float64 {
	// [NUOVO 16/11, punto 20] Senza metriche live si usano le misure del
	// tuning da file (tempo di init e probabilità di cold start), prima dei
	// ripieghi generici.
	if tuned && tv.initTime >= 0 {
		fallbackInit = tv.initTime
	}
	clampProb := func(p float64) float64 {
		if math.IsNaN(p) || math.IsInf(p, 1) {
			return 1.0
		}
		if p < 0.0 {
			return 0.0
		}
		return p
	}

	if c.key == "CLOUD" {
		prob := 0.0 // comportamento ereditato da ILP/HEFTless se manca ogni dato
		if p, found := rm.RemoteColdStartProbability[funcName]; found {
			prob = clampProb(p)
		} else if tuned && tv.coldStartProb >= 0 {
			prob = tv.coldStartProb // punto 20
		}
		if t, ok := rm.AvgRemoteInitTime[funcName]; ok {
			return t * prob
		}
		return fallbackInit * prob // nessun dato di init time: default.init.time (punto 18)
	}

	nId := node.NodeID{Area: c.area, Key: c.key}
	prob := 1.0
	if tuned && tv.coldStartProb >= 0 {
		prob = tv.coldStartProb // punto 20: sostituito dalla metrica live se presente
	}
	if nodeProbs, found := rm.EdgeColdStartProbability[nId.String()]; found {
		if p, found := nodeProbs[funcName]; found {
			prob = clampProb(p)
		}
	}
	if nodeTimes, found := rm.AvgEdgeInitTime[nId.String()]; found {
		if t, found := nodeTimes[funcName]; found {
			return t * prob
		}
	}
	return fallbackInit * prob // default.init.time (punto 18)
}

// ---------------------------------------------------------------------
// Trasferimento dati nodo a nodo (punto 23).
// ---------------------------------------------------------------------

// elDataSource dice dove si trova un dato (il nodo candidato che lo ha
// prodotto, o l'origine) e quanti byte pesa.
type elDataSource struct {
	loc   elCandidate
	bytes float64
	// viaOrigin: il dato, dopo essere stato prodotto su loc, è passato da un
	// task che gira sull'origine (Pass, ParallelTask, ...): il successore lo
	// riceve dall'origine anche se si trova sullo stesso nodo loc (punto 24).
	viaOrigin bool
	// [21/11, doppio conteggio] Identità del dato, per contare una sola
	// volta energia, CO2 e costo di una tratta che serve a più task:
	// producer è il task che lo ha prodotto su loc (il nodo remoto lo
	// restituisce all'origine una volta sola); item è il predecessore
	// immediato del task che lo riceve, cioè la chiave con cui il motore lo
	// invia (prepareOffloadData lo mette una volta per richiesta, anche se lo
	// usano più task offloadati insieme).
	producer TaskId
	item     TaskId
	// earlier: dato prodotto PRIMA di questa pianificazione (task già
	// eseguito o pianificato in precedenza, punto 27). Il nodo che lo ha
	// prodotto lo ha già restituito all'origine, che lo invia insieme alla
	// richiesta di offloading (prepareOffloadData manda gli output dei
	// predecessori di tutti i task del piano del nodo): pesa solo la tratta
	// origine -> nodo e non costringe gli altri input a tornare all'origine.
	earlier bool
}

// elDataFlow descrive i dati che un task deve ricevere e restituire, per
// stimarne i trasferimenti su ciascun candidato.
type elDataFlow struct {
	origin         elCandidate    // nodo da cui parte la richiesta (centro della stella)
	inputs         []elDataSource // dove si trovano gli input del task (uno per predecessore, più di uno per un join) e quanto pesano
	returnToOrigin bool           // ultimo task funzione: il risultato torna all'origine
	baseFunc       string         // funzione del task, per la dimensione dell'output di una variante senza metrica
	// shipped: tratte già pagate (energia, CO2, costo) da task posizionati
	// prima nello stesso piano; letta durante la stima, aggiornata da
	// buildPlan dopo ogni scelta (doppio conteggio, 21/11).
	shipped map[string]bool
}

// elTransfer è la stima dei trasferimenti di un task su un candidato.
type elTransfer struct {
	seconds float64
	energyJ float64
	carbonG float64
	cost    float64
}

// transferEstimate somma le tratte necessarie per eseguire il task sul
// candidato c, con la topologia a stella del branch (punto 23): ogni input va
// da dove si trova all'origine e dall'origine a c, saltando le tratte nulle;
// il risultato dell'ultimo task torna all'origine. Ritorna false se una
// tratta necessaria non ha banda configurata.
func (policy *EnergyLessPolicy) transferEstimate(flow elDataFlow, c elCandidate, outBytes float64, record map[string]bool) (elTransfer, bool) {
	var total elTransfer
	// add somma una tratta. Il TEMPO si conta sempre: un task deve comunque
	// aspettare che i dati arrivino, anche se viaggiano nella stessa
	// richiesta di un altro ramo. Energia, CO2 e costo invece si contano
	// una volta sola per trasferimento fisico (chiave key): se la tratta è
	// già stata pagata da un task posizionato prima, per questo task valgono
	// zero (correzione del doppio conteggio, 21/11). Con record != nil la
	// tratta viene registrata come pagata.
	add := func(src, dst elCandidate, timeBytes, energyBytes float64, keys []string) bool {
		t, ok := policy.hop(src, dst, timeBytes)
		if !ok {
			return false
		}
		total.seconds += t.seconds
		if energyBytes > 0 {
			e, _ := policy.hop(src, dst, energyBytes)
			total.energyJ += e.energyJ
			total.carbonG += e.carbonG
			total.cost += e.cost
		}
		if record != nil && src.key != dst.key {
			for _, k := range keys {
				record[k] = true
			}
		}
		return true
	}
	paid := func(key string) bool { return flow.shipped[key] }

	// [21/11, punto 24] Input da più predecessori (join di un fork). Se
	// tutti gli input sono già su c, prodotti lì senza passare dall'origine,
	// non c'è nessun trasferimento: i predecessori e il task girano nello
	// stesso segmento remoto. Altrimenti il nodo remoto non può eseguire il
	// task (per esempio il join non è eseguibile finché un ramo è altrove),
	// quindi restituisce i propri output all'origine: ogni input che sta su
	// un nodo diverso dall'origine fa la tratta verso l'origine, e poi
	// l'origine invia a c tutti gli input insieme.
	//
	// [22/11, punto 27] Gli input "earlier" (prodotti prima di questa
	// pianificazione) sono già sull'origine e viaggiano con la richiesta: non
	// impediscono agli altri input di restare sul nodo; pesano solo nella
	// tratta origine -> c.
	local := true
	var earlierBytes, earlierUnpaid float64
	var earlierKeys []string
	for _, in := range flow.inputs {
		if in.earlier {
			sendKey := "send|" + string(in.item) + "|" + c.key
			earlierBytes += in.bytes
			if !paid(sendKey) {
				earlierUnpaid += in.bytes
			}
			earlierKeys = append(earlierKeys, sendKey)
			continue
		}
		if in.loc.key != c.key || in.viaOrigin {
			local = false
		}
	}
	if local {
		if earlierBytes > 0 && c.key != flow.origin.key && !add(flow.origin, c, earlierBytes, earlierUnpaid, earlierKeys) {
			return total, false
		}
	} else {
		toC, toCUnpaid := earlierBytes, earlierUnpaid
		sendKeys := append([]string(nil), earlierKeys...)
		for _, in := range flow.inputs {
			if in.earlier {
				continue
			}
			if in.loc.key != flow.origin.key {
				// Ritorno all'origine dell'output di in.producer: una sola
				// volta, qualunque sia il numero di task che lo usano.
				backKey := "back|" + string(in.producer) + "|" + in.loc.key
				energyBytes := in.bytes
				if paid(backKey) {
					energyBytes = 0
				}
				if !add(in.loc, flow.origin, in.bytes, energyBytes, []string{backKey}) {
					return total, false
				}
			}
			// Invio dall'origine a c dell'output di in.item: una sola volta
			// per nodo destinatario.
			sendKey := "send|" + string(in.item) + "|" + c.key
			toC += in.bytes
			if !paid(sendKey) {
				toCUnpaid += in.bytes
			}
			sendKeys = append(sendKeys, sendKey)
		}
		if c.key != flow.origin.key && !add(flow.origin, c, toC, toCUnpaid, sendKeys) {
			return total, false
		}
	}
	if flow.returnToOrigin && c.key != flow.origin.key && !add(c, flow.origin, outBytes, outBytes, nil) {
		return total, false
	}
	return total, true
}

// hop stima una singola tratta src -> dst di bytes byte. Nulla se src e dst
// sono lo stesso nodo o non ci sono dati da spostare.
func (policy *EnergyLessPolicy) hop(src, dst elCandidate, bytes float64) (elTransfer, bool) {
	if src.key == dst.key || bytes <= 0 {
		return elTransfer{}, true
	}
	bandwidthMbps := linkBandwidthMbps(src, dst)
	if bandwidthMbps <= 0 {
		return elTransfer{}, false
	}
	txJ := bytes * src.txEnergyJPerByte
	rxJ := bytes * dst.rxEnergyJPerByte
	return elTransfer{
		seconds: bytes / (bandwidthMbps * 1_000_000 / 8),
		energyJ: txJ + rxJ,
		carbonG: (txJ/3.6e6)*src.co2Intensity + (rxJ/3.6e6)*dst.co2Intensity,
		cost:    bytes * policy.linkTransferCostPerByte(src, dst),
	}, true
}

// linkBandwidthMbps è la banda di una tratta, con le stesse chiavi e gli
// stessi default della RemoteOffloadingPolicy del main (commit 5107d34):
// edge_to_edge_bandwidth tra due nodi edge, node2datastore.bandwidth tra un
// nodo edge e il cloud.
func linkBandwidthMbps(src, dst elCandidate) float64 {
	if src.key == "CLOUD" || dst.key == "CLOUD" {
		return config.GetFloat(config.WORKFLOW_OFFLOADING_POLICY_NODE_TO_DATA_STORE_BANDWIDTH, 40.0)
	}
	return config.GetFloat(config.WORKFLOW_OFFLOADING_POLICY_EDGE_TO_EDGE_BANDWIDTH, 100.0)
}

// linkTransferCostPerByte è il costo per byte di una tratta (CT^f, Eq. 14):
// cloud2datastore.transfer.cost se la tratta tocca il cloud,
// node2datastore.transfer.cost tra nodi edge. Default 0.
func (policy *EnergyLessPolicy) linkTransferCostPerByte(src, dst elCandidate) float64 {
	if src.key == "CLOUD" || dst.key == "CLOUD" {
		return policy.cloudTransferCost
	}
	return policy.nodeTransferCost
}

// originCandidate è il candidato locale, cioè il nodo che sta pianificando
// (la ripianificazione avviene solo sull'origine: i nodi remoti ricevono
// CanDoOffloading=false).
func originCandidate(candidates []elCandidate) elCandidate {
	for _, c := range candidates {
		if c.isLocal {
			return c
		}
	}
	return elCandidate{isLocal: true}
}

// outputBytes è la dimensione media dell'output di una funzione o variante
// (metrica AvgOutputSize); per una variante senza misura si usa quella della
// funzione base. 0 se non c'è nessuna misura.
func outputBytes(name, baseFunc string) float64 {
	sizes := metrics.GetMetrics().AvgOutputSize
	if b, ok := sizes[name]; ok {
		return b
	}
	return sizes[baseFunc]
}

// predecessorOutputBytes stima l'input di un task pronto a metà esecuzione:
// la somma degli output medi dei FunctionTask che lo precedono (più di uno
// per un join), 0 se non ce ne sono.
func predecessorOutputBytes(w *Workflow, tid TaskId) float64 {
	return predecessorOutputBytesSeen(w, tid, make(map[TaskId]bool))
}

// taskOutputBytes è la dimensione media dell'output di un task: quella della
// sua funzione per un FunctionTask, l'input passato invariato per un task
// strutturale (punto 27).
func taskOutputBytes(w *Workflow, tid TaskId) float64 {
	if ft, ok := w.Tasks[tid].(*FunctionTask); ok {
		return outputBytes(ft.Func, ft.Func)
	}
	return predecessorOutputBytes(w, tid)
}

// taskFunctionName è la funzione base di un FunctionTask ("" per gli altri).
func taskFunctionName(w *Workflow, tid TaskId) string {
	if ft, ok := w.Tasks[tid].(*FunctionTask); ok {
		return ft.Func
	}
	return ""
}

// predecessorOutputBytesSeen risale anche attraverso i task strutturali
// (ChoiceTask, Pass, Fork), che passano i dati senza modificarli: il primo
// task del ramo scelto da un ChoiceTask riceve l'output della funzione che
// precede il ChoiceTask (punto 27).
func predecessorOutputBytesSeen(w *Workflow, tid TaskId, seen map[TaskId]bool) float64 {
	total := 0.0
	for _, prev := range w.GetPreviousTasks(tid) {
		if seen[prev] {
			continue
		}
		seen[prev] = true
		switch t := w.Tasks[prev].(type) {
		case *FunctionTask:
			total += outputBytes(t.Func, t.Func)
		case *StartTask:
			// parametri della richiesta: non noti qui, trascurati
		default:
			total += predecessorOutputBytesSeen(w, prev, seen)
		}
	}
	return total
}

// isLastFunctionTask dice se dopo ft, seguendo i task unari, si arriva alla
// fine del workflow senza altri FunctionTask: in quel caso il suo risultato
// torna all'origine. Davanti a un ChoiceTask o a un ParallelTask risponde
// false (il ramo non è noto staticamente: sarà rivisto con quei punti).
func isLastFunctionTask(w *Workflow, ft *FunctionTask) bool {
	next := ft.GetNext()
	for steps := 0; steps <= len(w.Tasks); steps++ {
		if next == "" {
			return true
		}
		switch t := w.Tasks[next].(type) {
		case *FunctionTask:
			return false
		case *EndTask:
			return true
		case UnaryTask:
			next = t.GetNext()
		default:
			return false
		}
	}
	return false
}

// energyJoules è la traduzione dell'Eq. 7 del paper (E^f) usata nella
// funzione obiettivo. Qui si scrive la formula per esteso, invece di
// richiamare internal/emissions.Compute(), perché quella funzione è pensata
// per il modello ad "edge aggregato" della policy CO2/QoS e non per un
// confronto diretto tra istanze candidate.
//
// [DECISO 18/11 con il prof. Russo Russo, applicato 20/11 — punto 22]
//   - Niente potenza idle ε_i: i nodi restano accesi comunque, quindi l'idle
//     non dipende dalla decisione di scheduling e non deve influenzarla.
//     Supera la decisione del 22/10 (idlePowerW * execTimeSec nell'energia).
//     Per il consumo totale a posteriori resta idleEnergyJoules().
//   - Ogni funzione usa 1 core CPU: energia di esecuzione = potenza di un core
//     × tempo (corePowerW), al posto di procPowerW × memoria/2048 × CPUDemand.
//
// [20/11, punto 23] Contiene solo l'energia di ESECUZIONE: quella dei dati
// (trasmissione/ricezione) si calcola tratta per tratta in transferEstimate()
// ed estimateOnCandidate() la somma.
func energyJoules(c elCandidate, execTimeSec float64) float64 {
	return corePowerW(c) * execTimeSec
}

// corePowerW è la potenza di un singolo core del candidato (punto 22):
// consumption.processing.power si riferisce all'intero nodo, quindi si divide
// per il numero di core. Se il numero di core è sconosciuto (<= 0) si usa la
// potenza del nodo intera, cioè si assume un nodo da 1 core: è il caso
// prudente (sovrastima l'energia invece di azzerarla).
func corePowerW(c elCandidate) float64 {
	if c.cores > 0 {
		return c.procPowerW / c.cores
	}
	return c.procPowerW
}

// idleEnergyJoules è l'energia idle ε_i × tempo. Dal 20/11 (punto 22) NON
// entra nella funzione obiettivo né nella CO2: resta solo per calcolare a
// posteriori il consumo totale effettivo negli esperimenti, come indicato dal
// prof. Russo Russo il 18/11.
func idleEnergyJoules(c elCandidate, execTimeSec float64) float64 {
	return c.idlePowerW * execTimeSec
}

// estimateExecTime riusa esattamente lo stesso approccio di
// remote_offloading_policy.go::prepareParameters() per stimare ψ_i^f dalle
// metriche Prometheus già raccolte (AvgEdgeExecutionTime / AvgRemoteExecutionTime).
// Ritorna 0 se non esiste ancora una metrica reale per (candidato, funzione-
// o-variante): il chiamante (estimateOnCandidate/rankReadyTasks) decide
// quale fallback applicare.
func estimateExecTime(rm metrics.RetrievedMetrics, c elCandidate, funcName string) float64 {
	if c.key == "CLOUD" {
		if t, ok := rm.AvgRemoteExecutionTime[funcName]; ok {
			return t
		}
		return 0
	}
	nId := node.NodeID{Area: c.area, Key: c.key}
	if nodeTimes, ok := rm.AvgEdgeExecutionTime[nId.String()]; ok {
		if t, ok := nodeTimes[funcName]; ok {
			return t
		}
	}
	return 0
}

// regionCost riusa la stessa config di ILP/HEFTless (region.cost), incluso
// lo stesso ToLower() dovuto al parsing case-insensitive di viper.
func regionCost(area string) float64 {
	costs := config.GetStringMapFloat64(config.WORKFLOW_OFFLOADING_POLICY_REGION_COST)
	if v, ok := costs[strings.ToLower(area)]; ok {
		return v
	}
	return 0.0
}

func safeDiv(v, max float64) float64 {
	if max <= 0 {
		return 0
	}
	return v / max
}

func maxFloat(a, b float64) float64 {
	if b > a {
		return b
	}
	return a
}

// ---------------------------------------------------------------------
// [CONFERMATO, non più da aggiungere]: workflow.go::CreateOffloadingPolicy()
// ha già il branch "energyless" -> &EnergyLessPolicy{} accanto a ilp/
// heftless/threshold.
// ---------------------------------------------------------------------

// ---------------------------------------------------------------------
// Domande ancora aperte (aggiornato dopo il ricevimento del 02/10):
//
//  1. [RISOLTO 01/11, confermato 06/11] Esecuzione effettiva della variante
//     scelta, vedi punto 16 in testa al file. Alternativa valutata il 02/11:
//     lasciare Fun alla funzione base e portare la variante in un campo
//     separato (function.Request.Variant) copiato in
//     scheduledRequest.executionTarget da SubmitRequest, con le policy di
//     scheduling che acquisiscono il container su executionFunction().
//     Implementata e provata a 2 nodi, poi ritirata il 06/11: Cecilia ha
//     detto che anche Fun = variante va bene, e la versione attuale tocca
//     meno codice del suo repository (utile per una futura pull request).
//     Altra alternativa scartata: riscrivere FunctionTask.Func, perché i task
//     sono puntatori condivisi tra tutte le richieste concorrenti dello
//     stesso workflow (Get()/getFromCache() copia solo in modo shallow la
//     struct Workflow, non i Task).
//  2. [PARZIALMENTE RISOLTO 30/10] Utility della variante (v.Utility): ora
//     usata come VINCOLO di qualità minima (prodotto di catena >= soglia,
//     punto 15 in testa al file). Resta aperta, come estensione opzionale
//     prevista dal ricevimento del 22/10, la sua integrazione anche come
//     quinto termine pesato dell'obiettivo (Eq. 22).
//  2b. [RISOLTO] Validità di una variante: ValidateVariantMetadata() esiste
//     davvero su *function.Function (verificato sul vero function.go) —
//     validVariants() la richiama di nuovo. L'errore di compilazione
//     "Unresolved reference" riscontrato prima dipendeva da una versione di
//     function.go non ancora aggiornata localmente, non da un'API
//     inesistente. Se l'errore dovesse ripresentarsi: verificare che il
//     function.go nel proprio albero locale/branch sia alla stessa versione
//     di quello qui controllato (pulire la cache di gopls/IDE e rifare
//     `go build` può bastare se è solo un problema di indicizzazione).
//  3. [RISOLTO] Chiave di cache del piano: confermata r.Id — visibile
//     direttamente in getCachedSolution(r *Request) di ILP/HEFTless
//     (placementCache[r.Id]), non più un'ipotesi.
//  4. Pulizia della cache: le voci scadono dopo N letture (ttl int, non più
//     tempo reale — vedi punto 6 in testa al file), ma solo "lazy": una
//     richiesta che smette di essere rivalutata prima di esaurire il
//     contatore (es. fallisce, o il nodo scelto resta sempre lo stesso per
//     poche letture) lascia comunque la entry in memoria fino a un'eventuale
//     pulizia esplicita. Stesso comportamento del placementCache originale
//     di ILP/HEFTless (non è un problema introdotto da questa bozza), quindi
//     non prioritario da risolvere qui per primi.
//  5. [RISOLTO, confermato da Cecilia via mail il 16/10] co2Intensity del
//     cloud: interrogata live in gatherCandidates() tramite
//     fetchCloudStatus(), che reimplementa localmente (non esportata da
//     scheduling) lo stesso pattern HTTP già usato in produzione da
//     fetchNodeStatus()/discoverCloudCandidates() in
//     internal/scheduling/co2_qos_policy.go: GET a
//     "<APIUrl del load balancer dell'area>/status", risposta decodificata
//     in registration.StatusInformation (campo CO2Intensity, reale e live).
//     Cecilia ha confermato che questo è esattamente il meccanismo corretto
//     (già presente come struct AreaStat in regions/types.go e già usato
//     da discoverCloudCandidates), quindi non c'è altro da costruire.
//     Fallback se la query fallisce (timeout 3s, cloud irraggiungibile,
//     status code non 200, JSON non valido): NON più la CO2Intensity del
//     nodo locale come in discoverCloudCandidates() — Cecilia ha chiarito
//     che quel fallback "silenzioso" non è una scelta di design validata,
//     solo un aspetto mai sistemato in quel codice, lasciando la scelta a
//     questa bozza. Si usa invece l'ULTIMO valore live ottenuto con
//     successo dal cloud (policy.lastKnownCloudCO2, con policy.cloudCO2Mu a
//     protezione), più accurato di un valore preso da un nodo diverso; il
//     nodo locale resta come stand-in SOLO alla primissima query fallita,
//     quando ancora non esiste nessun valore noto da riusare. Resta un
//     round-trip HTTP in più dentro Evaluate()/buildPlan(), ma non nel
//     percorso critico ad ogni round: gatherCandidates() è chiamata solo
//     quando il piano NON è già in cache (punto 6 in testa al file, TTL
//     effettivamente permanente), quindi al più una volta per richiesta,
//     non ad ogni round come in Co2QosAwarePolicy. Non toccati invece
//     procPowerW/txEnergyJPerByte/rxEnergyJPerByte del candidato cloud, che
//     restano letti da policy.regionsCfg come prima (fuori scope di questa
//     modifica, che riguardava solo la CO2).
//  6. [AGGIORNATO] cacheSolution/getCachedPlan ora replicano fedelmente la
//     logica reale di ILP/HEFTless (verificata sul file
//     remote_offloading_policy.go, non più un'ipotesi) ma restano una copia
//     locale e non una funzione condivisa, perché il tipo del valore in
//     cache è diverso: qui map[TaskId]*elEstimate, nell'originale
//     taskPlacement = map[TaskId]string. Un refactor verso un helper
//     generico condiviso resta possibile come lavoro successivo.
//  7. [RISOLTO nel ricevimento del 22/10, dalla voce "Da sistemare" su
//     costo/CO2 incompleti] Intensità di carbonio dello STORAGE (ξ_j, Eq.
//     10): il paper la tiene distinta da quella del candidato di calcolo
//     (ξ_i), ma rispetto all'equazione originale di EnergyLess (storage
//     esterno selezionabile per nodo, j ∈ S), in Serverledge si farà
//     riferimento a etcd/al modello di storage centralizzato — confermato
//     dal gruppo che non serve modellare una selezione di storage separati
//     né quindi una CO2Intensity distinta per lo storage.
//     estimateOnCandidate() continua ad approssimare ξ_j con ξ_i (vedi
//     commento lì), ora per scelta architetturale confermata, non più per
//     mancanza del dato. Costo di trasferimento (CT^f) e di storage (CS^f)
//     restano comunque aggiunti (punto 14 in testa al file) con due nuove
//     coppie di chiavi, già promosse in internal/config/keys.go.
//  8. [RISOLTO nel ricevimento del 22/10, dalla voce "Da sistemare" su
//     potenza idle] Reintegrata ε_i nell'Eq. 7 (elCandidate.idlePowerW,
//     in energyJoules) il 17/10, con
//     decisione rimasta in sospeso fino al ricevimento. Confermato dal
//     gruppo: sia la reintegrazione sia la forma dimensionalmente corretta
//     (idlePowerW * execTimeSec, Potenza × Tempo = Joule, non la somma
//     diretta letterale del paper) — applicato in energyJoules().
//  9. [IMPLEMENTATO 30/10, deciso 22/10] Soglia minima di accuratezza per le
//     varianti: vedi punto 15 in testa al file. Verificato sul vero
//     function.go/request.go che l'accuratezza per task esiste già
//     (Function.Utility) ma che function.RequestQoS NON aveva nessun campo
//     per la soglia: aggiunto MinAccuracy (da inserire nel repo reale),
//     default 95% se non impostato. Provato il 01/11 su un nodo reale: la
//     soglia passata dall'API HTTP (QoS.MinAccuracy) arriva alla policy. Da
//     fare ancora: (a) validare con Cecilia l'ingresso della soglia lato
//     client (il CLI invoke-workflow non ha un flag per MinAccuracy);
//     (b) eventuale termine di accuratezza nello score.
// ---------------------------------------------------------------------
