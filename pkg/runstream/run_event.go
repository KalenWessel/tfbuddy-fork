package runstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

const RunEventsStreamName = "RUN_EVENTS"

// TFRunEvent represents a status change on a run
type TFRunEvent struct {
	RunID        string
	Organization string
	Workspace    string
	NewStatus    string
	Metadata     RunMetadata
	Carrier      propagation.MapCarrier `json:"Carrier"`
	context      context.Context
}

func (e *TFRunEvent) GetRunID() string {
	return e.RunID
}
func (e *TFRunEvent) GetContext() context.Context {
	return e.context
}
func (e *TFRunEvent) SetContext(ctx context.Context) {
	e.context = ctx
}
func (e *TFRunEvent) SetCarrier(carrier map[string]string) {
	e.Carrier = carrier
}
func (e *TFRunEvent) GetNewStatus() string {
	return e.NewStatus
}
func (e *TFRunEvent) GetMetadata() RunMetadata {
	return e.Metadata
}
func (e *TFRunEvent) SetMetadata(meta RunMetadata) {
	e.Metadata = meta
}

func (s *Stream) PublishTFRunEvent(ctx context.Context, re RunEvent) error {
	ctx, span := otel.Tracer("TF").Start(ctx, "PublishTFRunEvent")
	defer span.End()
	re.SetContext(ctx)

	rmd, err := s.waitForTFRunMetadata(re)
	if err != nil {
		log.Error().Err(err).Msg("unable to publish TF Run Event: could not get RunMetadata")
		return err
	}

	b, err := encodeTFRunEvent(ctx, re)
	if err != nil {
		return err
	}

	// Anchor JetStream dedup to (runID, newStatus). TFC fires a webhook for
	// every configured notification trigger, and several triggers resolve to
	// the same RunStatus over the run's lifecycle (e.g. plan_queued, planning,
	// cost_estimating, policy_checking can all surface as `planning` in the
	// notification payload). Without this header, each redundant webhook
	// produced its own TFRunEvent and a duplicate GitLab reply.
	// Combined with the Duplicates window on the stream (see
	// configureTFRunEventsStream), this collapses retries and same-status
	// redeliveries server-side.
	msgID := tfRunEventMsgID(re.GetRunID(), re.GetNewStatus())
	_, err = s.js.Publish(
		fmt.Sprintf("%s.%s", RunEventsStreamName, rmd.GetVcsProvider()),
		b,
		nats.MsgId(msgID),
	)

	return err
}

// tfRunEventMsgID is the JetStream dedup key for a TFRunEvent. Package-level
// helper so the publish path and the in-package tests stay in lockstep — keep
// it unexported until another package actually needs to compute the same key.
func tfRunEventMsgID(runID, newStatus string) string {
	return runID + ":" + newStatus
}

func (s *Stream) SubscribeTFRunEvents(vcsProvider string, cb func(run RunEvent) bool) (closer func(), err error) {
	sub, err := s.js.QueueSubscribe(
		fmt.Sprintf("%s.%s", RunEventsStreamName, vcsProvider),
		vcsProvider,
		func(msg *nats.Msg) {
			re, err := decodeTFRunEvent(msg.Data)
			if err != nil {
				log.Error().Err(err).Msg("could not decode Run")
				if err := msg.Term(); err != nil {
					log.Error().Err(err).Msg("could not Terminate NATS msg")
				}
				return
			}
			log := log.With().
				Str("stream", RunEventsStreamName).
				Str("RunID", re.GetRunID()).Logger()

			if err := msg.InProgress(); err != nil {
				if err := msg.Nak(); err != nil {
					log.Error().Err(err).Msg("could not Nak NATS msg")
				}
				return
			}

			// Enrich TFRunEvent with TFRunMetadata from our KV
			rmd, err := s.waitForTFRunMetadata(re)
			if err != nil {
				if err := msg.Term(); err != nil {
					log.Error().Err(err).Msg("could not Terminate NATS msg")
				}
				return
			}
			re.SetMetadata(rmd)

			log.Debug().Msg("sending TFRunEvent to subscriber.")
			if cb(re) {
				log.Debug().Msg("ACKd.")
				if err := msg.Ack(); err != nil {
					log.Error().Err(err).Msg("could not Ack NATS msg")
				}
			} else {
				log.Debug().Msg("NACK.")
				if err := msg.Nak(); err != nil {
					log.Error().Err(err).Msg("could not Nak NATS msg")
				}
			}
		},
	)
	if err != nil {
		return nil, err
	}

	closer = func() {
		// TODO: unsure if we actually want this for a Queue that is shared across pods
		err := sub.Unsubscribe()
		if err != nil {
			log.Error().Err(err).Msg("could not unsubscribe from NATS queue")
		}
	}

	return closer, nil
}

// configureTFRunEventsStream provisions the RUN_EVENTS stream. dedupWindow
// sets the JetStream Duplicates window used in tandem with the Nats-Msg-Id
// stamped by PublishTFRunEvent. Operators tune the window via the
// TFBUDDY_JETSTREAM_DEDUP_WINDOW env var; tests pass an explicit value.
func configureTFRunEventsStream(js nats.JetStreamContext, dedupWindow time.Duration) {
	sCfg := &nats.StreamConfig{
		Name:        RunEventsStreamName,
		Description: "Terraform Cloud Run Notifications",
		Subjects:    []string{fmt.Sprintf("%s.*", RunEventsStreamName)},
		Retention:   nats.WorkQueuePolicy,
		MaxMsgs:     10240,
		MaxAge:      time.Hour * 6,
		Replicas:    1,
		Duplicates:  dedupWindow,
	}

	addOrUpdateStream(js, sCfg)
}

func (s *Stream) waitForTFRunMetadata(run RunEvent) (RunMetadata, error) {
	// Often when a Run is first created in TFC we receive the first webhook back from TFC before the
	// run metadata has been written to the Nats KV store. So we need to retry a few times.
	var md RunMetadata
	getMetadata := func() (err error) {
		md, err = s.GetRunMeta(run.GetRunID())
		return
	}

	err := backoff.Retry(getMetadata, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3))
	if err != nil {
		log.Warn().
			Err(err).
			Str("runID", run.GetRunID()).
			Msg("Run Metadata not retrieved from KV and attempts have been exhausted, giving up.")

		return nil, errors.New("Run Metadata not found, giving up")
	}

	return md, nil
}

func decodeTFRunEvent(b []byte) (RunEvent, error) {
	run := &TFRunEvent{}
	run.Metadata = &TFRunMetadata{}
	err := json.Unmarshal(b, &run)
	run.context = otel.GetTextMapPropagator().Extract(context.Background(), run.Carrier)
	return run, err
}

func encodeTFRunEvent(ctx context.Context, run RunEvent) ([]byte, error) {
	carrier := propagation.MapCarrier(map[string]string{})
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	run.SetCarrier(carrier)
	return json.Marshal(run)
}
