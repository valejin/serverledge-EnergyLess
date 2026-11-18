package workflow

import (
	"bytes"
	"errors"
	"io"
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

// ---------------------------------------------------------------------
// Valori di tuning salvati su file (punto 20 in energyless_policy.go).
//
// Indicazione di Cecilia (11/11): fare un tuning preliminare ed usare i
// valori stimati in quella fase. Le metriche Prometheus però si perdono se
// Prometheus o il Pushgateway vengono riavviati e, per gli edge, sono
// indicizzate per chiave del nodo, che cambia a ogni riavvio del nodo: le
// misure di un tuning precedente non sarebbero più riconducibili al nodo.
//
// Lo script scripts/tune-energyless.sh scrive quindi le misure in un file
// YAML, per funzione e per TIPO di nodo (edge/cloud), e la policy lo legge
// all'avvio (chiave tuning.file nel file YAML della policy). Ordine usato per
// ogni stima: metrica live da Prometheus -> valore del file di tuning ->
// default (default.exec.time / default.init.time, punto 18).
//
// Formato:
//
//	measured: "2026-11-16 11:30"        # informativo
//	functions:
//	  euler_standard:
//	    edge:  {exec.time: 4.107, init.time: 0.764, cold.start.prob: 0.10}
//	    cloud: {exec.time: 3.950, init.time: 0.812, cold.start.prob: 0.10}
//	  euler_light:
//	    edge:  {exec.time: 0.054, init.time: 0.536, cold.start.prob: 0.10}
// ---------------------------------------------------------------------

const (
	tuningKindEdge  = "edge"
	tuningKindCloud = "cloud"
)

// tunedValues sono le misure del tuning per (funzione, tipo di nodo). Un
// valore negativo significa "non misurato".
type tunedValues struct {
	execTime      float64 // secondi
	initTime      float64 // secondi
	coldStartProb float64 // in [0, 1]
}

// energyLessTuning: funzione (o variante) -> tipo di nodo -> misure.
type energyLessTuning map[string]map[string]tunedValues

type tuningFileEntry struct {
	ExecTime      *float64 `yaml:"exec.time"`
	InitTime      *float64 `yaml:"init.time"`
	ColdStartProb *float64 `yaml:"cold.start.prob"`
}

type tuningFileFunction struct {
	Edge  *tuningFileEntry `yaml:"edge"`
	Cloud *tuningFileEntry `yaml:"cloud"`
}

type tuningFileContent struct {
	Measured  string                        `yaml:"measured"`
	Functions map[string]tuningFileFunction `yaml:"functions"`
}

// loadEnergyLessTuning legge il file di tuning. Valori fuori intervallo
// vengono scartati con un avviso (restano "non misurati").
func loadEnergyLessTuning(path string) (energyLessTuning, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var fc tuningFileContent
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&fc); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	convert := func(fn, kind string, e *tuningFileEntry) (tunedValues, bool) {
		if e == nil {
			return tunedValues{}, false
		}
		tv := tunedValues{execTime: -1, initTime: -1, coldStartProb: -1}
		if e.ExecTime != nil {
			if *e.ExecTime > 0 {
				tv.execTime = *e.ExecTime
			} else {
				log.Printf("EnergyLess: tuning %s/%s exec.time=%v non valido (deve essere > 0), ignorato", fn, kind, *e.ExecTime)
			}
		}
		if e.InitTime != nil {
			if *e.InitTime >= 0 {
				tv.initTime = *e.InitTime
			} else {
				log.Printf("EnergyLess: tuning %s/%s init.time=%v non valido (deve essere >= 0), ignorato", fn, kind, *e.InitTime)
			}
		}
		if e.ColdStartProb != nil {
			if *e.ColdStartProb >= 0 && *e.ColdStartProb <= 1 {
				tv.coldStartProb = *e.ColdStartProb
			} else {
				log.Printf("EnergyLess: tuning %s/%s cold.start.prob=%v non valido (deve essere in [0,1]), ignorato", fn, kind, *e.ColdStartProb)
			}
		}
		return tv, true
	}

	t := make(energyLessTuning, len(fc.Functions))
	for fn, f := range fc.Functions {
		kinds := make(map[string]tunedValues, 2)
		if tv, ok := convert(fn, tuningKindEdge, f.Edge); ok {
			kinds[tuningKindEdge] = tv
		}
		if tv, ok := convert(fn, tuningKindCloud, f.Cloud); ok {
			kinds[tuningKindCloud] = tv
		}
		if len(kinds) > 0 {
			t[fn] = kinds
		}
	}
	log.Printf("EnergyLess: tuning letto da %s (misurato %q): %d funzioni", path, fc.Measured, len(t))
	return t, nil
}

// lookup ritorna le misure di tuning per la funzione sul tipo di nodo del
// candidato (cloud per il candidato CLOUD, edge per tutti gli altri).
func (t energyLessTuning) lookup(c elCandidate, funcName string) (tunedValues, bool) {
	if t == nil {
		return tunedValues{}, false
	}
	kind := tuningKindEdge
	if c.key == "CLOUD" {
		kind = tuningKindCloud
	}
	tv, ok := t[funcName][kind]
	return tv, ok
}
