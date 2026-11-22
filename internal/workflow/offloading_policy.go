package workflow

type OffloadingDecision struct {
	Offload    bool   `json:"offload"`
	RemoteHost string `json:"remote_host"`
	OffloadingPlan
}

type OffloadingPolicy interface {
	Init()
	Evaluate(r *Request, p *Progress, runningTasks map[TaskId]bool) ([]OffloadingDecision, error)
}

// RequestCompletionListener is an optional interface for offloading policies
// that keep per-request state (e.g., a cached plan) or per-request statistics.
// If the configured policy implements it, Invoke calls RequestCompleted on the
// node where the request was submitted (the one that plans) when the workflow
// execution ends: err is nil if the workflow completed successfully, otherwise
// it is the error returned by Invoke.
type RequestCompletionListener interface {
	RequestCompleted(r *Request, err error)
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

func (policy *NoOffloadingPolicy) Evaluate(r *Request, p *Progress, runningTasks map[TaskId]bool) ([]OffloadingDecision, error) {

	return []OffloadingDecision{{Offload: false}}, nil
}

// SetOffloadingPolicy allows replacing the global offloading policy.
// NOTE: This function should only be used in tests to inject a mock policy.
func SetOffloadingPolicy(policy OffloadingPolicy) {
	offloadingPolicy = policy
}
