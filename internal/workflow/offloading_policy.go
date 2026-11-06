package workflow

type OffloadingDecision struct {
	Offload    bool   `json:"offload"`
	RemoteHost string `json:"remote_host"`
	OffloadingPlan
}

type OffloadingPolicy interface {
	Init()
	Evaluate(r *Request, p *Progress) (OffloadingDecision, error)
}

type OffloadingPlan struct {
	ToExecute []TaskId
	// Variants: variante da eseguire per i task di ToExecute che non usano la
	// funzione base (task -> nome della variante). Opzionale.
	Variants map[TaskId]string
}

type NoOffloadingPolicy struct{}

func (policy *NoOffloadingPolicy) Init() {
}

func (policy *NoOffloadingPolicy) Evaluate(r *Request, p *Progress) (OffloadingDecision, error) {

	return OffloadingDecision{Offload: false}, nil
}
