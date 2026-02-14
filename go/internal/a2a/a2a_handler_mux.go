package a2a

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
	authimpl "github.com/kagent-dev/kagent/go/internal/httpserver/auth"
	common "github.com/kagent-dev/kagent/go/internal/utils"
	"github.com/kagent-dev/kagent/go/pkg/auth"
	"trpc.group/trpc-go/trpc-a2a-go/client"
	"trpc.group/trpc-go/trpc-a2a-go/server"
)

// A2AHandlerMux is an interface that defines methods for adding, getting, and removing agentic task handlers.
type A2AHandlerMux interface {
	SetAgentHandler(
		agentRef string,
		client *client.A2AClient,
		card server.AgentCard,
	) error
	RemoveAgentHandler(
		agentRef string,
	)
	http.Handler
}

type handlerMux struct {
	handlers       map[string]http.Handler
	lock           sync.RWMutex
	basePathPrefix string
	authenticator  auth.AuthProvider
	authorizer     auth.Authorizer
}

var _ A2AHandlerMux = &handlerMux{}

// respondWithJSONError writes a JSON error response in the standard format
func respondWithJSONError(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]string{"error": message}) //nolint:errcheck
}

func NewA2AHttpMux(pathPrefix string, authenticator auth.AuthProvider, authorizer auth.Authorizer) *handlerMux {
	return &handlerMux{
		handlers:       make(map[string]http.Handler),
		basePathPrefix: pathPrefix,
		authenticator:  authenticator,
		authorizer:     authorizer,
	}
}

func (a *handlerMux) SetAgentHandler(
	agentRef string,
	client *client.A2AClient,
	card server.AgentCard,
) error {
	srv, err := server.NewA2AServer(card, NewPassthroughManager(client), server.WithMiddleWare(authimpl.NewA2AAuthenticator(a.authenticator)))
	if err != nil {
		return fmt.Errorf("failed to create A2A server: %w", err)
	}

	a.lock.Lock()
	defer a.lock.Unlock()

	a.handlers[agentRef] = srv.Handler()

	return nil
}

func (a *handlerMux) RemoveAgentHandler(
	agentRef string,
) {
	a.lock.Lock()
	defer a.lock.Unlock()
	delete(a.handlers, agentRef)
}

func (a *handlerMux) getHandler(name string) (http.Handler, bool) {
	a.lock.RLock()
	defer a.lock.RUnlock()
	handler, ok := a.handlers[name]
	return handler, ok
}

func (a *handlerMux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	// get the handler name from the first path segment
	agentNamespace, ok := vars["namespace"]
	if !ok || agentNamespace == "" {
		respondWithJSONError(w, http.StatusBadRequest, "Agent namespace not provided")
		return
	}
	agentName, ok := vars["name"]
	if !ok || agentName == "" {
		respondWithJSONError(w, http.StatusBadRequest, "Agent name not provided")
		return
	}

	handlerName := common.ResourceRefString(agentNamespace, agentName)

	// Check authorization if authorizer is configured
	if a.authorizer != nil {
		session, ok := auth.AuthSessionFrom(r.Context())
		if !ok {
			respondWithJSONError(w, http.StatusUnauthorized, "Unauthorized: no valid session found")
			return
		}
		resource := auth.Resource{
			Type: "Agent",
			Name: handlerName,
		}
		if err := a.authorizer.Check(r.Context(), session.Principal(), auth.VerbGet, resource); err != nil {
			respondWithJSONError(w, http.StatusForbidden, fmt.Sprintf("Forbidden: %v", err))
			return
		}
	}

	// get the underlying handler
	handlerHandler, ok := a.getHandler(handlerName)
	if !ok {
		respondWithJSONError(w, http.StatusNotFound, fmt.Sprintf("Agent %s not found", handlerName))
		return
	}

	handlerHandler.ServeHTTP(w, r)
}
