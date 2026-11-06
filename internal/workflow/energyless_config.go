package workflow

import (
	"bytes"
	"errors"
	"io"
	"log"
	"os"

	"gopkg.in/yaml.v3"

	"github.com/serverledge-faas/serverledge/internal/config"
)

// ---------------------------------------------------------------------
// Configurazione di EnergyLessPolicy da file YAML dedicato.
//
// Stesso schema di Co2QosAwarePolicy (chiave "policy.general.config.path" +
// LoadPolicyConfig + yaml.Unmarshal in internal/scheduling): il percorso del
// file è indicato nel file di configurazione principale del nodo con la
// chiave WORKFLOW_OFFLOADING_POLICY_ENERGYLESS_CONFIG_PATH, e i parametri
// della policy stanno in un file a parte.
//
// Priorità dei valori, dal più forte al più debole:
//  1. (solo soglia di accuratezza) RequestQoS.MinAccuracy della richiesta;
//  2. valore presente nel file YAML dedicato;
//  3. chiave già esistente nel file di configurazione principale
//     (workflow.offloading.policy.energyless.*), mantenuta come fallback per
//     non rompere le configurazioni già in uso;
//  4. default scritto nel codice.
//
// Il file è facoltativo: se la chiave del percorso manca, o il file non è
// leggibile/valido, la policy parte comunque con i valori ai punti 3-4 (e lo
// segnala nel log) invece di fallire.
// ---------------------------------------------------------------------

// energyLessFileConfig è il contenuto del file YAML. I campi sono puntatori
// per distinguere "chiave assente" da "valore 0" (0 è un valore legittimo,
// ad es. per annullare il peso di un termine dell'obiettivo).
type energyLessFileConfig struct {
	WeightTime   *float64 `yaml:"weight.time"`
	WeightEnergy *float64 `yaml:"weight.energy"`
	WeightCarbon *float64 `yaml:"weight.carbon"`
	WeightCost   *float64 `yaml:"weight.cost"`

	PlacementTTL       *int     `yaml:"placement.ttl"`
	DefaultMinAccuracy *float64 `yaml:"default.min.accuracy"`

	NodeTransferCost  *float64 `yaml:"node2datastore.transfer.cost"`
	CloudTransferCost *float64 `yaml:"cloud2datastore.transfer.cost"`
	NodeStorageCost   *float64 `yaml:"node2datastore.storage.cost"`
	CloudStorageCost  *float64 `yaml:"cloud2datastore.storage.cost"`
}

// energyLessParams sono i parametri risolti (file > chiavi principali >
// default) che Init() copia nella policy.
type energyLessParams struct {
	weights            energyLessWeights
	placementTTL       int
	defaultMinAccuracy float64
	nodeTransferCost   float64
	cloudTransferCost  float64
	nodeStorageCost    float64
	cloudStorageCost   float64
}

// loadEnergyLessParams risolve i parametri della policy. Parte dai valori
// letti dalle chiavi principali (con gli stessi default di prima, quindi
// nessun cambio di comportamento senza file) e li sovrascrive con quelli del
// file YAML dedicato, se configurato.
func loadEnergyLessParams() energyLessParams {
	p := energyLessParams{
		weights: energyLessWeights{
			time:   config.GetFloat(config.WORKFLOW_OFFLOADING_POLICY_ENERGYLESS_WEIGHT_TIME, 0.25),
			energy: config.GetFloat(config.WORKFLOW_OFFLOADING_POLICY_ENERGYLESS_WEIGHT_ENERGY, 0.25),
			carbon: config.GetFloat(config.WORKFLOW_OFFLOADING_POLICY_ENERGYLESS_WEIGHT_CARBON, 0.25),
			cost:   config.GetFloat(config.WORKFLOW_OFFLOADING_POLICY_ENERGYLESS_WEIGHT_COST, 0.25),
		},
		placementTTL:       config.GetInt(config.WORKFLOW_OFFLOADING_POLICY_ENERGYLESS_PLACEMENT_TTL, 9999),
		defaultMinAccuracy: defaultMinAccuracy,
		nodeTransferCost:   config.GetFloat(config.WORKFLOW_OFFLOADING_POLICY_ENERGYLESS_NODE_TO_DATA_STORE_TRANSFER_COST, 0.0),
		cloudTransferCost:  config.GetFloat(config.WORKFLOW_OFFLOADING_POLICY_ENERGYLESS_CLOUD_TO_DATA_STORE_TRANSFER_COST, 0.0),
		nodeStorageCost:    config.GetFloat(config.WORKFLOW_OFFLOADING_POLICY_ENERGYLESS_NODE_TO_DATA_STORE_STORAGE_COST, 0.0),
		cloudStorageCost:   config.GetFloat(config.WORKFLOW_OFFLOADING_POLICY_ENERGYLESS_CLOUD_TO_DATA_STORE_STORAGE_COST, 0.0),
	}

	path := config.GetString(config.WORKFLOW_OFFLOADING_POLICY_ENERGYLESS_CONFIG_PATH, "")
	if path == "" {
		log.Printf("EnergyLess: no dedicated config file (%s not set), using main config keys and defaults",
			config.WORKFLOW_OFFLOADING_POLICY_ENERGYLESS_CONFIG_PATH)
		logEnergyLessParams(p, "")
		return p
	}

	fc, err := readEnergyLessFileConfig(path)
	if err != nil {
		log.Printf("EnergyLess: could not load config file %q: %v (using main config keys and defaults)", path, err)
		logEnergyLessParams(p, "")
		return p
	}

	nonNegative := func(v float64) bool { return v >= 0 }
	overrideFloat(&p.weights.time, fc.WeightTime, "weight.time", nonNegative)
	overrideFloat(&p.weights.energy, fc.WeightEnergy, "weight.energy", nonNegative)
	overrideFloat(&p.weights.carbon, fc.WeightCarbon, "weight.carbon", nonNegative)
	overrideFloat(&p.weights.cost, fc.WeightCost, "weight.cost", nonNegative)
	overrideFloat(&p.nodeTransferCost, fc.NodeTransferCost, "node2datastore.transfer.cost", nonNegative)
	overrideFloat(&p.cloudTransferCost, fc.CloudTransferCost, "cloud2datastore.transfer.cost", nonNegative)
	overrideFloat(&p.nodeStorageCost, fc.NodeStorageCost, "node2datastore.storage.cost", nonNegative)
	overrideFloat(&p.cloudStorageCost, fc.CloudStorageCost, "cloud2datastore.storage.cost", nonNegative)
	overrideFloat(&p.defaultMinAccuracy, fc.DefaultMinAccuracy, "default.min.accuracy",
		func(v float64) bool { return v > 0 && v <= 1 })

	if fc.PlacementTTL != nil {
		if *fc.PlacementTTL > 0 {
			p.placementTTL = *fc.PlacementTTL
		} else {
			log.Printf("EnergyLess: invalid placement.ttl=%d in %q (must be > 0), ignored", *fc.PlacementTTL, path)
		}
	}

	logEnergyLessParams(p, path)
	return p
}

// readEnergyLessFileConfig legge e valida la struttura del file YAML. Con
// KnownFields(true) una chiave scritta male (es. "weigth.time") produce un
// errore invece di essere ignorata in silenzio. Un file vuoto è valido
// (equivale a "nessuna sovrascrittura").
func readEnergyLessFileConfig(path string) (energyLessFileConfig, error) {
	var fc energyLessFileConfig
	data, err := os.ReadFile(path)
	if err != nil {
		return fc, err
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&fc); err != nil && !errors.Is(err, io.EOF) {
		return energyLessFileConfig{}, err
	}
	return fc, nil
}

// overrideFloat copia *src in *dst se la chiave era presente nel file (src !=
// nil) e il valore è valido; un valore non valido viene scartato con un
// avviso e resta quello di fallback.
func overrideFloat(dst *float64, src *float64, key string, valid func(float64) bool) {
	if src == nil {
		return
	}
	if !valid(*src) {
		log.Printf("EnergyLess: invalid %s=%v in config file, ignored", key, *src)
		return
	}
	*dst = *src
}

func logEnergyLessParams(p energyLessParams, path string) {
	source := "main config keys/defaults"
	if path != "" {
		source = "file " + path
	}
	log.Printf("EnergyLess: parameters from %s: weights(time=%.3g energy=%.3g carbon=%.3g cost=%.3g) placementTTL=%d defaultMinAccuracy=%.3g",
		source, p.weights.time, p.weights.energy, p.weights.carbon, p.weights.cost, p.placementTTL, p.defaultMinAccuracy)
}
