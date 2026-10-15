// Decisioni (aggiornato dopo il ricevimento del 02/10 con Cardellini/Cecilia/Russo Russo):
//  1. Decomposizione NON verrà implementata — sostituita dalla selezione tra
//     funzione base e varianti (Function.DefaultFunction/SpeedUp/Utility).
//     Implementata in selectBest(): per ogni candidato si valuta sia la
//     funzione base sia ogni sua variante valida, e si sceglie la coppia
//     (candidato, funzione-o-variante) con l'obiettivo minimo. ATTENZIONE:
//     la sostituzione EFFETTIVA della funzione eseguita (quando vince una
//     variante) non è ancora cablata nell'esecuzione reale — vedi "Domande
//     ancora aperte" in fondo al file.
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
//     estimateOnCandidate). La co2Intensity del cloud, invece, ora viene
//     interrogata live (fetchCloudStatus(), stesso pattern HTTP GET
//     "<APIUrl>/status" di fetchNodeStatus() in
//     internal/scheduling/co2_qos_policy.go), con fallback alla
//     CO2Intensity del nodo locale se il cloud non risponde — vedi
//     gatherCandidates().
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
// [CONFERMATO sul vero internal/config/keys.go, letto nello zip del
// progetto]: le 5 chiavi WORKFLOW_OFFLOADING_POLICY_ENERGYLESS_* (weight.
// time/.energy/.carbon/.cost + placement.ttl) sono già presenti, non più da
// aggiungere. Confermata anche la registrazione della policy in
// workflow.go::CreateOffloadingPolicy() (branch "energyless") — vedi anche
// il blocco in fondo al file, ora aggiornato di conseguenza.
//
// Non serve nient'altro: costo, banda e CO2 riusano chiavi già esistenti
// (region.cost, node2datastore.bandwidth, cloud2datastore.bandwidth,
// consumption.processing.power/.energy.tx/.rx, regions.file.path).
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
}

func (policy *EnergyLessPolicy) Init() {
	policy.weights = energyLessWeights{
		time:   config.GetFloat(config.WORKFLOW_OFFLOADING_POLICY_ENERGYLESS_WEIGHT_TIME, 0.25),
		energy: config.GetFloat(config.WORKFLOW_OFFLOADING_POLICY_ENERGYLESS_WEIGHT_ENERGY, 0.25),
		carbon: config.GetFloat(config.WORKFLOW_OFFLOADING_POLICY_ENERGYLESS_WEIGHT_CARBON, 0.25),
		cost:   config.GetFloat(config.WORKFLOW_OFFLOADING_POLICY_ENERGYLESS_WEIGHT_COST, 0.25),
	}

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
	policy.cacheTTLInit = config.GetInt(config.WORKFLOW_OFFLOADING_POLICY_ENERGYLESS_PLACEMENT_TTL, 9999)
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
	procPowerW       float64 // W — τ_i nel paper (potenza sotto carico, non distingue idle: deciso 02/10 di ignorare il termine idle, come nel paper)
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
	energyJoules   float64       // E^f, Eq. 7 (senza componente idle, deciso 02/10)
	carbonGrams    float64       // CO^f, Eq. 10
	cost           float64       // Z^f, Eq. 13
	objective      float64       // somma pesata normalizzata, Eq. 22
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

	return policy.computeDecisionFromPlacement(p, placement), nil
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

	for len(queue) > 0 {
		tid := queue[0]
		queue = queue[1:]
		if _, seen := placement[tid]; seen {
			continue
		}

		// Filtra solo i FunctionTask
		ft, isFunc := r.W.Tasks[tid].(*FunctionTask)
		if !isFunc {
			// Task non-funzione (Choice, Pass, ecc.): fine della catena
			// lineare a partire da qui — nessuna decisione di placement
			// da prendere, Serverledge lo eseguirà localmente.
			continue
		}
		f, found := function.GetFunction(ft.Func)
		if !found {
			continue
		}

		// Chiama selectBest() (righe 16-23 dell'Alg. 1, esteso con le
		// varianti — punto 1) per trovare la coppia migliore per questo
		// task. Se nessuna è fattibile (capacità o deadline), logga e
		// passa al prossimo task della coda — non abortisce l'intero piano
		// per un solo task problematico.
		best, err := policy.selectBest(f, candidates, residualDeadline)
		if err != nil {
			log.Printf("EnergyLess: nessuna coppia fattibile per %s (task %s): %v", ft.Func, tid, err)
			continue
		}
		if best.functionName != f.Name {
			// Ha vinto una variante: tracciato solo a scopo diagnostico per
			// ora — la sostituzione effettiva della funzione eseguita non è
			// ancora cablata, vedi "Domande ancora aperte" in fondo al file.
			log.Printf("EnergyLess: per il task %s scelta la variante %s al posto di %s", tid, best.functionName, f.Name)
		}
		placement[tid] = best

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
func (policy *EnergyLessPolicy) computeDecisionFromPlacement(p *Progress, placement map[TaskId]*elEstimate) OffloadingDecision {
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

	// Si ritorna una decisione di offload positiva: RemoteHost è l'URL del nodo scelto,
	// e OffloadingPlan.ToExecute è la lista COMPLETA dei task di competenza di quel nodo.
	return OffloadingDecision{
		Offload:    true,
		RemoteHost: chosenRemote.apiURL,
		OffloadingPlan: OffloadingPlan{
			ToExecute: toExecute,
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
		// prima approssimata con la CO2Intensity del nodo locale come
		// stand-in fisso; ora si tenta una query live allo stesso endpoint
		// "/status" del load balancer cloud, con lo stesso pattern HTTP già
		// usato in produzione da discoverCloudCandidates()/fetchNodeStatus()
		// in internal/scheduling/co2_qos_policy.go (fetchNodeStatus non è
		// esportata da quel package, quindi qui è reimplementata localmente
		// in fetchCloudStatus(), stesso endpoint, stesso timeout 3s, stesso
		// tipo di risposta registration.StatusInformation). Se la query
		// fallisce (timeout, cloud irraggiungibile, status code non 200,
		// JSON non decodificabile) si ricade sulla CO2Intensity del nodo
		// locale come prima — stesso comportamento di fallback "silenzioso"
		// di discoverCloudCandidates(), che non interrompe mai la pipeline
		// se il nodo di cui chiede lo stato non risponde.
		cloudCO2 := node.LocalResources.CO2Intensity() // fallback, come prima
		if status, err := fetchCloudStatus(*cloud); err == nil && status != nil {
			cloudCO2 = status.CO2Intensity
		} else if err != nil {
			log.Printf("energyless: query live CO2Intensity del cloud (%s) fallita, uso CO2Intensity locale come stand-in: %v", cloud.APIUrl(), err)
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
// Verificato sul vero internal/function/function.go condiviso da Valentina
// (package function):
//   - GetVariantsOf(baseName string) ([]*Function, error): ritorna solo le
//     funzioni già filtrate con IsVariant() && DefaultFunction == baseName
//     (vedi filterVariants), ordinate per nome — da qui la necessità di
//     gestire anche l'errore, non solo il value, come corretto sotto.
//   - ValidateVariantMetadata() esiste davvero come metodo su *Function
//     (controlla: non-default con DefaultFunction vuoto, nome diverso dal
//     default, SpeedUp > 0, Utility in [0,1]) — il precedente "Unresolved
//     reference" segnalato da Valentina non dipendeva quindi da un'API
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
func (policy *EnergyLessPolicy) selectBest(f *function.Function, candidates []elCandidate, residualDeadline float64) (*elEstimate, error) {
	rm := metrics.GetMetrics()
	variants := validVariants(f)
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
			accumulate(vEst)
			// NOTA: v.Utility non è ancora integrato nell'obiettivo (Eq. 22
			// ha solo 4 termini) né usato come vincolo di qualità minima —
			// resta una domanda di modellazione aperta, vedi fondo file.
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

	completion := time.Duration(execTime * float64(time.Second))
	if !c.isLocal {
		completion += transferTime(c, fn)
	}
	if !math.IsInf(residualDeadline, 1) && completion.Seconds() > residualDeadline {
		return execTime, nil, false // vincolo di deadline, Eq. 5
	}

	energyJ := energyJoules(c, fn, execTime)
	carbonG := (energyJ / 3.6e6) * c.co2Intensity // Eq. 10, stessa conversione J->kWh di emissions.joulesToKWh
	cost := regionCost(c.area) * execTime         // Eq. 13, ΔC_i · ψ_i^f

	return execTime, &elEstimate{
		candidate:      c,
		functionName:   metricName,
		completionTime: completion,
		energyJoules:   energyJ,
		carbonGrams:    carbonG,
		cost:           cost,
	}, true
}

// energyJoules è la traduzione diretta dell'Eq. 7 del paper (E^f), senza il
// termine di potenza idle ε_i: deciso 02/10 di ignorarlo per ora, in linea
// col paper originale. Qui si scrive la formula per esteso, invece di
// richiamare internal/emissions.Compute(), perché quella funzione è
// pensata per il modello ad "edge aggregato" della policy CO2/QoS
// (parametri InitialNode/LocalNode/AggrInitialNodeMemory) e non per un
// confronto diretto tra istanze candidate come qui serve.
func energyJoules(c elCandidate, f *function.Function, execTimeSec float64) float64 {
	const memoryUnitMB = 2048.0 // stessa costante di internal/emissions/emissions.go
	cpuUsage := f.CPUDemand
	if cpuUsage <= 0 {
		cpuUsage = 1.0
	}
	computeEnergy := c.procPowerW * (float64(f.MemoryMB) / memoryUnitMB) * execTimeSec * cpuUsage

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

	return computeEnergy + transferEnergy
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

// transferTime stima il tempo di trasferimento dati verso un'istanza non
// locale, riusando le stesse chiavi di banda già configurate per ILP/HEFTless.
func transferTime(c elCandidate, f *function.Function) time.Duration {
	outputBytes := metrics.GetMetrics().AvgOutputSize[f.Name]
	var bandwidthMbps float64
	if c.key == "CLOUD" {
		bandwidthMbps = config.GetFloat(config.WORKFLOW_OFFLOADING_POLICY_CLOUD_TO_DATA_STORE_BANDWIDTH, 1000)
	} else {
		bandwidthMbps = config.GetFloat(config.WORKFLOW_OFFLOADING_POLICY_NODE_TO_DATA_STORE_BANDWIDTH, 40.0)
	}
	if bandwidthMbps <= 0 {
		return 0
	}
	bandwidthBytesPerSec := bandwidthMbps * 1_000_000 / 8
	return time.Duration(outputBytes / bandwidthBytesPerSec * float64(time.Second))
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
// heftless/threshold, verificato leggendo lo zip del progetto.
// ---------------------------------------------------------------------

// ---------------------------------------------------------------------
// Domande ancora aperte (aggiornato dopo il ricevimento del 02/10):
//
//  1. [AGGIORNATO] Esecuzione effettiva della variante scelta: quando
//     selectBest() seleziona una variante invece della funzione base, oggi
//     questa bozza si limita a loggarlo (buildPlan) — non esiste ancora un
//     modo verificato per comunicare a Serverledge "esegui la variante X al
//     posto del task Y" attraverso OffloadingDecision/OffloadingPlan (che
//     hanno solo RemoteHost + ToExecute []TaskId, niente campo per il nome
//     funzione). Le 3 opzioni erano: (a) estendere OffloadingPlan, (b)
//     riscrivere temporaneamente FunctionTask.Func, (c) meccanismo già
//     esistente nel branch reale.
//     Verificato leggendo internal/workflow/function_task.go reale: (c) è
//     escluso con certezza — execute()/exec() leggono sempre e solo
//     s.Func, nessun override da Request/Progress/altrove.
//     (b) è stata anche ESCLUSA, non solo "da verificare": FunctionTask è
//     sempre un puntatore (Task "must be pointers to a struct", task.go),
//     e Workflow.Tasks è una map[TaskId]Task. Get()/getFromCache() in
//     workflow.go, in caso di cache hit, fa "fc := *cachedObj.(*Workflow)"
//     — una copia SOLO shallow: il campo Tasks (una mappa) viene copiato
//     per riferimento, non per valore, quindi tutte le richieste
//     concorrenti sullo stesso workflow condividono gli stessi puntatori
//     *FunctionTask. Mutare ft.Func qui cambierebbe la funzione eseguita
//     per TUTTE le altre richieste in corso o successive su quel workflow
//     — un bug di concorrenza, non solo una scelta discutibile (il
//     commento del codice reale, "return a safe copy", è impreciso su
//     questo punto: safe per la struct Workflow, non per i Task contenuti).
//     Resta quindi (a): portare "task Y -> variante X" per-richiesta
//     (estendendo OffloadingPlan/Request o un meccanismo analogo a come
//     viaggiano già TaskData/Progress), non nella definizione statica e
//     condivisa del workflow. Da disegnare e discutere con Cecilia — resta
//     il gap principale della feature varianti.
//  2. Utility della variante (v.Utility): non ancora integrata né come
//     quinto termine dell'obiettivo né come vincolo di qualità minima —
//     resta una scelta di modellazione aperta.
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
//  5. [RISOLTO] co2Intensity del cloud: ora interrogata live in
//     gatherCandidates() tramite fetchCloudStatus(), che reimplementa
//     localmente (non esportata da scheduling) lo stesso pattern HTTP già
//     usato in produzione da fetchNodeStatus()/discoverCloudCandidates() in
//     internal/scheduling/co2_qos_policy.go: GET a
//     "<APIUrl del load balancer dell'area>/status", risposta decodificata
//     in registration.StatusInformation (campo CO2Intensity, reale e live).
//     Se la query fallisce (timeout 3s, cloud irraggiungibile, status code
//     non 200, JSON non valido) si ricade sulla CO2Intensity del nodo
//     locale come prima — stesso comportamento di fallback "silenzioso" di
//     discoverCloudCandidates(), che non interrompe mai la pipeline se il
//     nodo interrogato non risponde. Resta un round-trip HTTP in più dentro
//     Evaluate()/buildPlan(), ma non nel percorso critico ad ogni round:
//     gatherCandidates() è chiamata solo quando il piano NON è già in cache
//     (punto 6 in testa al file, TTL effettivamente permanente), quindi al
//     più una volta per richiesta, non ad ogni round come in
//     Co2QosAwarePolicy. Se Cecilia/Cardellini preferissero evitare anche
//     questo singolo round-trip, si torna al solo fallback locale
//     commentando la chiamata a fetchCloudStatus() in gatherCandidates().
//     Non toccati invece procPowerW/txEnergyJPerByte/rxEnergyJPerByte del
//     candidato cloud, che restano letti da policy.regionsCfg come prima
//     (fuori scope di questa modifica, che riguardava solo la CO2).
//  6. [AGGIORNATO] cacheSolution/getCachedPlan ora replicano fedelmente la
//     logica reale di ILP/HEFTless (verificata sul file
//     remote_offloading_policy.go, non più un'ipotesi) ma restano una copia
//     locale e non una funzione condivisa, perché il tipo del valore in
//     cache è diverso: qui map[TaskId]*elEstimate, nell'originale
//     taskPlacement = map[TaskId]string. Un refactor verso un helper
//     generico condiviso resta possibile come lavoro successivo.
// ---------------------------------------------------------------------
