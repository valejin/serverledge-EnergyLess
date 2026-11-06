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
//     workflow. Implementato in computeDecisionFromPlacement(): toExecute
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
//     dataStoreTransferCostPerByte/dataStoreStorageCostPerByte), default
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
	nodeStorageCost    float64
	cloudStorageCost   float64

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
	availCPU         float64 // idem — per CLOUD si assume capacità infinita (deciso 02/10)
	co2Intensity     float64 // gCO2eq/kWh — ξ_i nel paper
	procPowerW       float64 // W — τ_i nel paper (potenza sotto carico, esecuzione)
	idlePowerW       float64 // W — ε_i nel paper (potenza a riposo, nodo attivo ma senza task in esecuzione). [RIVISTO 17/10] Reintegrata in energyJoules() — la decisione 02/10 di ignorarla è stata superata, vedi punto 5 in testa al file. [CONFERMATO 22/10] Forma scalata per execTimeSec (Potenza×Tempo=Joule), vedi energyJoules()
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
	energyJoules   float64       // E^f, Eq. 7 (include ε_i idle dal 17/10, vedi energyJoules())
	carbonGrams    float64       // CO^f, Eq. 10 (termine storage ξ_j: approssimato con ξ_i, confermato corretto 22/10 — vedi estimateOnCandidate)
	cost           float64       // Z^f = CC^f + CT^f + CS^f, Eq. 13+14+15
	accuracy       float64       // ∈[0,1], [NUOVO 30/10] accuratezza della funzione/variante scelta rispetto a quella richiesta dal task: 1.0 per la funzione del task, Function.Utility per una variante sostituta (vedi selectBest)
	objective      float64       // somma pesata normalizzata, Eq. 22
}

// defaultMinAccuracy è la soglia minima di accuratezza del workflow usata
// quando la richiesta non ne specifica una (RequestQoS.MinAccuracy <= 0).
// [NUOVO 30/10, deciso nel ricevimento del 22/10] Valore provvisorio al 95%
// scelto "per il momento": promuoverlo a chiave di
// configurazione (come i pesi γ1..γ4) se servirà variarlo negli esperimenti.
const defaultMinAccuracy = 0.95

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

// Prende in input la richiesta attuale (Request) e lo stato di avanzamento (Progress) e
// restituisce una decisione (OffloadingDecision) e un eventuale errore (error).
func (policy *EnergyLessPolicy) Evaluate(r *Request, p *Progress) (OffloadingDecision, error) {
	if p == nil || !r.CanDoOffloading || len(p.ReadyToExecute) == 0 {
		return OffloadingDecision{Offload: false}, nil
	}

	// Piano in cache (punto 6): se esiste già un placement valido per questa
	// richiesta, lo si usa direttamente senza ricalcolare nulla.
	placement, ok := policy.getCachedPlan(r.Id)
	if !ok {
		candidates := policy.gatherCandidates()
		if len(candidates) == 0 {
			return OffloadingDecision{Offload: false}, nil
		}

		residualDeadline := math.Inf(1)
		if r.QoS.MaxRespT > 0.0 {
			residualDeadline = r.QoS.MaxRespT - time.Since(r.Arrival).Seconds()
		}

		// Calcola il placement per TUTTA la catena di task non ancora
		// eseguiti, una sola volta, e lo cachea.
		placement = policy.buildPlan(r, p, candidates, residualDeadline)
		policy.cacheSolution(r.Id, placement)
	}

	// Varianti scelte (punto 16): scritte una sola volta per richiesta, quando
	// il campo non è ancora valorizzato, così non ci sono scritture
	// concorrenti con i task già in esecuzione. variantChoices ritorna sempre
	// una mappa non nil, anche se vuota.
	if r.Variants == nil {
		r.Variants = variantChoices(r.W, placement)
	}

	return policy.computeDecisionFromPlacement(p, placement, r.Variants), nil
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
func (policy *EnergyLessPolicy) buildPlan(r *Request, p *Progress, candidates []elCandidate, residualDeadline float64) map[TaskId]*elEstimate {
	placement := make(map[TaskId]*elEstimate)

	// Ranking dei task pronti (righe 6-12, Alg. 1): r_f = tempo medio
	// stimato sulle istanze candidate. NOTA: TODO non propaga ancora il rank dei
	// predecessori (riga 11) — con la topologia attuale (catene lineari o
	// Choice, niente FanOut) ReadyToExecute contiene quasi sempre un solo
	// task per round, quindi l'ordine tra più task pronti conta poco; da
	// rivedere quando FanOut sarà disponibile.
	queue := policy.rankReadyTasks(r, p, candidates)

	// elapsedChain accumula il completionTime (Eq. 2-4) dei task già
	// posizionati in QUESTA chiamata di buildPlan, camminando la catena.
	// Serve a calcolare, per il prossimo task, la deadline RESIDUA corretta
	// ("residualDeadline - elapsedChain") invece di passare sempre la
	// stessa residualDeadline calcolata una sola volta in Evaluate() —
	// bug del 17/10 (punto 13 in testa al file):
	// senza questo accumulo un task a metà catena viene valutato come se
	// fosse l'unico rimasto, e un piano con più task singolarmente entro
	// deadline può comunque sforare la deadline complessiva della
	// richiesta.
	elapsedChain := 0.0

	// [NUOVO 30/10, deciso nel ricevimento del 22/10] Soglia minima di
	// accuratezza e accumulatore di catena, speculari a deadline/elapsedChain:
	// l'accuratezza complessiva del workflow è il PRODOTTO delle accuratezze
	// dei task (∈[0,1]), e deve restare >= minAccuracy. accChain è il prodotto
	// dei task già posizionati in QUESTA chiamata. Poiché la funzione del task
	// ha sempre accuratezza 1.0 (vedi selectBest), i task rimanenti possono
	// sempre "restituire" il budget: richiedere accChain * acc(task) >= soglia
	// a ogni passo basta quindi a garantire che il prodotto finale non scenda
	// sotto soglia. Limite noto: la scelta resta greedy per task (come l'Alg.
	// 1), quindi un task iniziale può consumare tutto il budget e costringere
	// i successivi alla funzione base, senza ottimizzare il budget sull'intera
	// catena.
	minAccuracy := policy.minAccuracyThreshold(r)
	accChain := 1.0

	for len(queue) > 0 {
		tid := queue[0]
		queue = queue[1:]
		if _, seen := placement[tid]; seen {
			continue
		}

		task := r.W.Tasks[tid]

		// Filtra solo i FunctionTask per il placement — ma NON si ferma
		// qui per gli altri tipi di task, come faceva prima (bug del
		// 17/10, verificato sul vero task.go/progress.go):
		// InitProgress() mette SEMPRE in ReadyToExecute, al round 1, solo
		// lo StartTask (workflow.Start.Id) — che non è un *FunctionTask.
		// Il codice precedente faceva "continue" su questo mismatch SENZA
		// accodare il successore, quindi la coda si vuotava subito, il
		// placement usciva vuoto, e quell'empty map veniva cachata con TTL
		// 9999 (quasi permanente) — la policy non offloadava mai più per
		// l'intera vita della richiesta. StartTask/PassTask/FailureTask
		// (oltre a FunctionTask) implementano tutti UnaryTask (hanno un
		// GetNext() deterministico, verificato su start_task.go/
		// pass_task.go/fail_task.go/function_task.go): per questi si segue
		// GetNext() senza fare placement (non eseguono codice utente, non
		// c'è nessuna decisione da prendere). Ci si ferma DAVVERO solo per
		// un ConditionalTask (ChoiceTask: il prossimo task dipende da dati
		// a runtime, non calcolabile staticamente qui) o per un task senza
		// alcun successore (EndTask e simili).
		ft, isFunc := task.(*FunctionTask)
		if !isFunc {
			if ut, isUnary := task.(UnaryTask); isUnary {
				if next := ut.GetNext(); next != "" {
					queue = append(queue, next)
				}
			}
			continue
		}
		f, found := function.GetFunction(ft.Func)
		if !found {
			continue
		}

		// Budget residuo per QUESTO task (punto 13 in testa al file): la
		// deadline complessiva della richiesta meno quanto già "consumato"
		// dai task precedenti nella stessa catena (elapsedChain) — non più
		// la residualDeadline originale, invariata per tutta la catena.
		remainingDeadline := residualDeadline
		if !math.IsInf(residualDeadline, 1) {
			remainingDeadline = residualDeadline - elapsedChain
		}

		// Chiama selectBest() (righe 16-23 dell'Alg. 1, esteso con le
		// varianti — punto 1) per trovare la coppia migliore per questo
		// task. Se nessuna è fattibile (capacità o deadline residua, ora
		// correttamente al netto della catena già percorsa), logga e passa
		// al prossimo task della coda — non abortisce l'intero piano per un
		// solo task problematico.
		//
		// Accuratezza minima richiesta a QUESTO task: da accChain * acc >=
		// minAccuracy segue acc >= minAccuracy / accChain. Dato che accChain
		// resta >= minAccuracy (invariante, vedi sopra) il rapporto è <= 1.
		minTaskAccuracy := minAccuracy / accChain
		best, err := policy.selectBest(f, candidates, remainingDeadline, minTaskAccuracy)
		if err != nil {
			log.Printf("EnergyLess: nessuna coppia fattibile per %s (task %s): %v", ft.Func, tid, err)
			continue
		}
		if best.functionName != f.Name {
			// Ha vinto una variante: oltre al log, la scelta resta in
			// placement[tid].functionName e viene resa esecutiva da Evaluate()
			// (variantChoices) e da FunctionTask.exec(), vedi punto 16.
			log.Printf("EnergyLess: per il task %s scelta la variante %s al posto di %s (accuratezza task %.3f, di catena %.3f, soglia %.3f)",
				tid, best.functionName, f.Name, best.accuracy, accChain*best.accuracy, minAccuracy)
		}
		placement[tid] = best
		elapsedChain += best.completionTime.Seconds()
		accChain *= best.accuracy

		if ft.NextTask != "" {
			queue = append(queue, ft.NextTask)
		}
	}

	return placement
}

// computeDecisionFromPlacement traduce il placement già calcolato (in
// cache) in una OffloadingDecision per il round corrente — stesso ruolo di
// computeDecisionFromPlacement in ILP/HEFTless, ma qui senza richiamare
// selectBest(): legge solo quanto già deciso in buildPlan().
func (policy *EnergyLessPolicy) computeDecisionFromPlacement(p *Progress, placement map[TaskId]*elEstimate, variants map[TaskId]string) OffloadingDecision {
	// Fase 1 — SOLO per decidere SE offloadare questo round e VERSO QUALE
	// nodo: si guardano ancora i task pronti ora (p.ReadyToExecute), stessa
	// logica greedy di prima (locale vince subito; altrimenti il primo task
	// pronto remoto fissa chosenRemote). Questa parte NON decide più il
	// contenuto del piano da inviare — solo il nodo di destinazione.
	localWins := false
	var chosenRemote *elCandidate

	for _, tid := range p.ReadyToExecute {
		est, ok := placement[tid]
		if !ok {
			// Task non coperto dal piano cachato (es. nuovo ramo dopo un
			// ChoiceTask non previsto in buildPlan): lasciato in locale per
			// questo round, non forza un ricalcolo del piano intero.
			continue
		}

		// Se il candidato migliore per il task corrente è il nodo locale, si ferma tutto il ciclo (break).
		if est.candidate.isLocal {
			localWins = true
			break // stesso comportamento di computeDecisionFromPlacement in ILP/HEFTless: locale vince subito
		}

		// Se invece il candidato migliore è remoto: chosenRemote viene aggiornato a OGNI task
		// pronto remoto incontrato (non solo al primo). Verificato sul codice reale di
		// computeDecisionFromPlacement in remote_offloading_policy.go (ILP/HEFTless):
		//   for _, t := range p.ReadyToExecute {
		//       assignedNode := placement[t]
		//       if assignedNode == registration.SelfRegistration.Key { localExecution = true; break }
		//       else { remoteNode = assignedNode }
		//   }
		// cioè se p.ReadyToExecute contiene più task pronti assegnati a nodi remoti diversi, vince
		// l'ULTIMO incontrato nell'ordine dello slice, non il primo — replicato qui identicamente
		// per restare fedeli al comportamento originale (anche se non è l'unica scelta sensata).
		c := est.candidate
		chosenRemote = &c
	}

	// Se localWins è vero (un task ha "vinto" localmente) oppure non è mai stato trovato nessun
	// candidato remoto valido
	if localWins || chosenRemote == nil {
		return OffloadingDecision{Offload: false}
	}

	// Fase 2 — chiarito nel ricevimento del 02/10 (punto 11 nell'header):
	// l'offloading plan NON si limita ai task pronti ora o consecutivi —
	// include TUTTI i task del piano cachato assegnati a chosenRemote per
	// l'intera richiesta (visione totale). Il nodo remoto, ricevuto il
	// piano, esegue i task i cui dati di input sono già disponibili e, se
	// incontra un task ancora in attesa di dati prodotti altrove, risponde
	// subito ignorandolo temporaneamente; il nodo sorgente prosegue con le
	// altre fasi del workflow. Per questo si itera su TUTTO placement, non
	// solo su p.ReadyToExecute. L'ordine è reso deterministico (utile per
	// log/debug ed esperimenti riproducibili) ordinando le chiavi come
	// stringa, dato che TaskId non è garantito avere un ordine naturale.
	toExecute := make([]TaskId, 0, len(placement))
	for tid, est := range placement {
		if est.candidate.key == chosenRemote.key {
			toExecute = append(toExecute, tid)
		}
	}
	sort.Slice(toExecute, func(i, j int) bool {
		return fmt.Sprintf("%v", toExecute[i]) < fmt.Sprintf("%v", toExecute[j])
	})

	// Le varianti scelte per i task offloadati viaggiano nel piano (punto 16):
	// il nodo remoto non ripianifica, esegue ciò che l'origine ha deciso.
	var planVariants map[TaskId]string
	for _, tid := range toExecute {
		if v, chosen := variants[tid]; chosen {
			if planVariants == nil {
				planVariants = make(map[TaskId]string)
			}
			planVariants[tid] = v
		}
	}

	// Si ritorna una decisione di offload positiva: RemoteHost è l'URL del nodo scelto,
	// e OffloadingPlan.ToExecute è la lista COMPLETA dei task di competenza di quel nodo.
	return OffloadingDecision{
		Offload:    true,
		RemoteHost: chosenRemote.apiURL,
		OffloadingPlan: OffloadingPlan{
			ToExecute: toExecute,
			Variants:  planVariants,
		},
	}
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
func (policy *EnergyLessPolicy) cacheSolution(requestId string, placement map[TaskId]*elEstimate) {
	policy.cacheMu.Lock()
	defer policy.cacheMu.Unlock()
	policy.cache[requestId] = &energyLessCacheEntry{
		placement: placement,
		ttl:       policy.cacheTTLInit,
	}
}

func (policy *EnergyLessPolicy) getCachedPlan(requestId string) (map[TaskId]*elEstimate, bool) {
	policy.cacheMu.Lock()
	defer policy.cacheMu.Unlock()

	entry, found := policy.cache[requestId]
	if !found {
		return nil, false
	}

	// Stesso comportamento di getCachedSolution: la entry si consuma una
	// lettura per volta; quando il contatore arriva a 0 viene eliminata.
	if entry.ttl > 0 {
		entry.ttl--
		return entry.placement, true
	}

	delete(policy.cache, requestId)
	return nil, false
}

// rankReadyTasks ordina i task pronti per rank decrescente (tempo medio
// stimato più alto = priorità più alta, stile HEFT/upward-rank).
// Ranking dei task pronti (righe 6-12, Alg. 1): r_f = tempo medio
// stimato sulle istanze candidate.
func (policy *EnergyLessPolicy) rankReadyTasks(r *Request, p *Progress, candidates []elCandidate) []TaskId {
	type ranked struct {
		id   TaskId
		rank float64
	}
	rm := metrics.GetMetrics()
	ranks := make([]ranked, 0, len(p.ReadyToExecute))

	for _, tid := range p.ReadyToExecute {
		ft, isFunc := r.W.Tasks[tid].(*FunctionTask)
		if !isFunc {
			ranks = append(ranks, ranked{id: tid, rank: 0})
			continue
		}
		var sum float64
		for _, c := range candidates {
			t := estimateExecTime(rm, c, ft.Func)
			if t <= 0 {
				t = 0.01 // nessuna metrica reale ancora disponibile: stesso fallback generico di prima
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
			availCPU:         node.LocalResources.AvailableCPUs(),
			co2Intensity:     node.LocalResources.CO2Intensity(),
			procPowerW:       node.LocalResources.ProcessingPower(),
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
			availCPU:         status.TotalCPU - status.UsedCPU,
			co2Intensity:     status.CO2Intensity,
			procPowerW:       status.ProcessingPowerConsumption,
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
		}
		if policy.regionsCfg != nil {
			if info, ok := policy.regionsCfg.Area(cloud.Area); ok {
				c.procPowerW = info.ProcessingPowerConsumption
				c.idlePowerW = info.IdlePowerConsumption
				c.txEnergyJPerByte = info.TxEnergyConsumption
				c.rxEnergyJPerByte = info.RxEnergyConsumption
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
func (policy *EnergyLessPolicy) selectBest(f *function.Function, candidates []elCandidate, residualDeadline float64, minTaskAccuracy float64) (*elEstimate, error) {
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
		baseExecTime, baseEst, ok := policy.estimateOnCandidate(rm, c, f, f.Name, residualDeadline, 0.01)
		if ok {
			baseEst.accuracy = 1.0 // la funzione del task non è degradata
			accumulate(baseEst)
		}

		// Varianti della stessa funzione, sullo stesso candidato (punto 1).
		for _, v := range variants {
			fallback := 0.01
			if ok && v.SpeedUp > 0 {
				// Approssimazione quando non esistono ancora metriche reali
				// per la variante su questo candidato: tempo della funzione
				// base scalato per lo SpeedUp dichiarato — stesso approccio
				// usato in internal/scheduling/co2_qos_optimizer.go
				// (populateOptimizerVariant) per lo stesso problema.
				fallback = baseExecTime / v.SpeedUp
			}
			_, vEst, vOk := policy.estimateOnCandidate(rm, c, v, v.Name, residualDeadline, fallback)
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
		e.objective = policy.weights.time*safeDiv(e.completionTime.Seconds(), maxTime) +
			policy.weights.energy*safeDiv(e.energyJoules, maxEnergy) +
			policy.weights.carbon*safeDiv(e.carbonGrams, maxCarbon) +
			policy.weights.cost*safeDiv(e.cost, maxCost)
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
// una metrica reale per (candidato, metricName): 0.01s per la funzione
// base, baseExecTime/SpeedUp per una variante (vedi selectBest).
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
func (policy *EnergyLessPolicy) estimateOnCandidate(rm metrics.RetrievedMetrics, c elCandidate, fn *function.Function, metricName string, residualDeadline float64, fallbackExecTime float64) (execTime float64, est *elEstimate, ok bool) {
	// Vincolo di capacità (Eq. 18-19 / ρ_i, Eq. 21 — chiarito 02/10 che ρ_i
	// corrisponde ad available CPU, già controllato qui): per il candidato
	// CLOUD il controllo è saltato interamente, perché si assume capacità
	// infinita (deciso 02/10, vedi gatherCandidates()).
	if c.key != "CLOUD" {
		if float64(fn.MemoryMB) > c.availMemMB || fn.CPUDemand > c.availCPU {
			return 0, nil, false
		}
	}

	execTime = estimateExecTime(rm, c, metricName)
	if execTime <= 0 {
		execTime = fallbackExecTime
	}
	if execTime <= 0 {
		execTime = 0.01
	}

	// Cold start (λ_i^f, Eq. 2): tempo di init ATTESO, già pesato per la
	// probabilità di cold start osservata — vedi estimateInitTime(). Prima
	// di questa modifica il piano ignorava del tutto questo termine,
	// sottostimando sistematicamente il completionTime di ogni candidato
	// (di più per i remoti, dove un container caldo è meno probabile che in
	// locale).
	initTime := estimateInitTime(rm, c, metricName)
	completion := time.Duration((execTime + initTime) * float64(time.Second))

	var transferCost, storageCost float64
	if !c.isLocal {
		// Vincolo di banda (Eq. 20): se per questo TIPO di candidato
		// (edge/nodo vs cloud) non è configurata una banda verso il data
		// store, il candidato non è fattibile — non si tratta più un
		// collegamento non configurato come "a tempo zero" (comportamento
		// della precedente transferTime(), che favoriva artificialmente un
		// candidato remoto mal configurato facendolo apparire più
		// veloce/economico di uno locale).
		bandwidthMbps := dataStoreBandwidthMbps(c)
		if bandwidthMbps <= 0 {
			return execTime, nil, false
		}
		// Stessa proxy approssimata già usata in energyJoules() (nessuna
		// dimensione di input per-task propagata: si riusa la dimensione
		// media di OUTPUT della funzione come stand-in sia per il dato da
		// scaricare — CT^f, δ^f — sia per quello da salvare — CS^f, δe^f).
		dataBytes := metrics.GetMetrics().AvgOutputSize[fn.Name]
		bandwidthBytesPerSec := bandwidthMbps * 1_000_000 / 8
		completion += time.Duration(dataBytes / bandwidthBytesPerSec * float64(time.Second))

		// Costo di trasferimento (CT^f, Eq. 14) e di storage (CS^f, Eq. 15):
		// entrambi 0 per esecuzione locale (stessa convenzione già in uso
		// per transferEnergy in energyJoules — nessun movimento di rete né,
		// per lo storage locale/edge, un costo da modellare: coerente con
		// l'esclusione "j ∈ S \ SE" di Eq. 15 nel paper, che tiene lo
		// storage edge fuori dal costo). Chiavi di configurazione NUOVE
		// (vedi dataStoreTransferCostPerByte/dataStoreStorageCostPerByte),
		// default 0.0: finché non vengono impostate esplicitamente, nessun
		// cambio di comportamento per chi già usa questa bozza.
		transferCost = policy.dataStoreTransferCostPerByte(c) * dataBytes
		storageCost = policy.dataStoreStorageCostPerByte(c) * dataBytes
	}

	if !math.IsInf(residualDeadline, 1) && completion.Seconds() > residualDeadline {
		return execTime, nil, false // vincolo di deadline, Eq. 5
	}

	energyJ := energyJoules(c, fn, execTime)
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
	carbonG := (energyJ / 3.6e6) * c.co2Intensity                    // Eq. 10, stessa conversione J->kWh di emissions.joulesToKWh
	cost := regionCost(c.area)*execTime + transferCost + storageCost // Eq. 13 (ΔC_i · ψ_i^f) + Eq. 14 + Eq. 15

	return execTime, &elEstimate{
		candidate:      c,
		functionName:   metricName,
		completionTime: completion,
		energyJoules:   energyJ,
		carbonGrams:    carbonG,
		cost:           cost,
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
func estimateInitTime(rm metrics.RetrievedMetrics, c elCandidate, funcName string) float64 {
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
		prob := clampProb(rm.RemoteColdStartProbability[funcName])
		if t, ok := rm.AvgRemoteInitTime[funcName]; ok {
			return t * prob
		}
		return 0.01 * prob // nessun dato di init time: stessa stima generica "a naso" usata altrove in questo file
	}

	nId := node.NodeID{Area: c.area, Key: c.key}
	prob := 1.0
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
	return 0.01 * prob
}

// Chiavi di configurazione NUOVE per il costo di trasferimento (Eq. 14) e di
// storage (Eq. 15). [AGGIORNATO 17/10] Promosse a costanti
// condivise in internal/config/keys.go (WORKFLOW_OFFLOADING_POLICY_ENERGYLESS_
// NODE/CLOUD_TO_DATA_STORE_TRANSFER/STORAGE_COST, stesso schema
// "node2datastore"/"cloud2datastore" già usato per la banda) — non più
// costanti locali non esportate come nella prima versione di questa
// modifica. Stessi valori stringa, nessun cambio di comportamento. Default
// 0.0 — finché non vengono impostate esplicitamente in configurazione,
// costo di trasferimento e di storage restano a zero.

// dataStoreBandwidthMbps/dataStoreTransferCostPerByte/dataStoreStorageCostPerByte
// centralizzano la scelta tra configurazione "verso il cloud" e "verso un
// nodo edge/locale", usata sia per il vincolo di banda e il tempo di
// trasferimento sia per i nuovi termini di costo — un solo punto invece di
// ripetere lo stesso "if c.key == CLOUD" in più punti del file.
func dataStoreBandwidthMbps(c elCandidate) float64 {
	if c.key == "CLOUD" {
		return config.GetFloat(config.WORKFLOW_OFFLOADING_POLICY_CLOUD_TO_DATA_STORE_BANDWIDTH, 1000)
	}
	return config.GetFloat(config.WORKFLOW_OFFLOADING_POLICY_NODE_TO_DATA_STORE_BANDWIDTH, 40.0)
}

func (policy *EnergyLessPolicy) dataStoreTransferCostPerByte(c elCandidate) float64 {
	if c.key == "CLOUD" {
		return policy.cloudTransferCost
	}
	return policy.nodeTransferCost
}

func (policy *EnergyLessPolicy) dataStoreStorageCostPerByte(c elCandidate) float64 {
	if c.key == "CLOUD" {
		return policy.cloudStorageCost
	}
	return policy.nodeStorageCost
}

// energyJoules è la traduzione diretta dell'Eq. 7 del paper (E^f). Qui si
// scrive la formula per esteso, invece di richiamare
// internal/emissions.Compute(), perché quella funzione è pensata per il
// modello ad "edge aggregato" della policy CO2/QoS (parametri
// InitialNode/LocalNode/AggrInitialNodeMemory) e non per un confronto
// diretto tra istanze candidate come qui serve.
//
// [RIVISTO 17/10] Include ora anche il
// termine di potenza idle ε_i (Eq. 7: "Ef = ϵi + τi·ψᶠᵢ + ϑi·(δf+δef)") —
// la decisione del 02/10 di ignorarlo "in linea col paper originale" è
// stata superata (vedi punto 5 in testa al file).
//
// [RISOLTO nel ricevimento del 22/10 con Cardellini/Cecilia/Russo Russo]
// Nell'Eq. 7 del paper ε_i compare come termine additivo semplice, non
// moltiplicato per execTimeSec — eppure la tabella delle notazioni lo
// definisce come una POTENZA (W), non un'energia (J), la stessa unità di
// τ_i che invece viene moltiplicato per il tempo. Confermata
// l'incongruenza dimensionale già segnalata; il gruppo ha
// deciso che nel codice il consumo energetico va calcolato correttamente
// come Potenza × Tempo di esecuzione (in secondi) = Joule, quindi
// idlePowerW * execTimeSec, non più la somma diretta "letterale"
// precedentemente implementata in attesa di conferma.
func energyJoules(c elCandidate, f *function.Function, execTimeSec float64) float64 {
	const memoryUnitMB = 2048.0 // stessa costante di internal/emissions/emissions.go
	cpuUsage := f.CPUDemand
	if cpuUsage <= 0 {
		cpuUsage = 1.0
	}
	computeEnergy := c.procPowerW * (float64(f.MemoryMB) / memoryUnitMB) * execTimeSec * cpuUsage
	idleEnergy := c.idlePowerW * execTimeSec // ε_i, Eq. 7 — Potenza × Tempo = Joule, confermato 22/10 (vedi nota sopra)

	outputBytes := metrics.GetMetrics().AvgOutputSize[f.Name]
	// L'input non è propagato per-task (stesso limite di remote_offloading_policy.go,
	// che usa r.ParamsSize come proxy per l'intero workflow); qui si usa la
	// dimensione media di output come proxy anche per l'input quando non è
	// il primo task, un'approssimazione grezza da rivedere.
	inputBytes := outputBytes

	transferEnergy := inputBytes*c.rxEnergyJPerByte + outputBytes*c.txEnergyJPerByte
	if c.isLocal {
		transferEnergy = 0 // nessun trasferimento di rete per l'esecuzione locale
	}

	return computeEnergy + idleEnergy + transferEnergy
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
