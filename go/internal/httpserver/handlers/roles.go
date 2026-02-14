package handlers

import (
	"net/http"

	"github.com/kagent-dev/kagent/go/api/v1alpha2"
	"github.com/kagent-dev/kagent/go/internal/httpserver/errors"
	"github.com/kagent-dev/kagent/go/pkg/database"
	"github.com/kagent-dev/kagent/go/pkg/client/api"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
)

// RolesHandler handles role-related requests
type RolesHandler struct {
	*Base
}

// NewRolesHandler creates a new RolesHandler
func NewRolesHandler(base *Base) *RolesHandler {
	return &RolesHandler{Base: base}
}

// CreateRoleRequest represents the request body for creating a role
type CreateRoleRequest struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// UpdateRoleRequest represents the request body for updating a role
type UpdateRoleRequest struct {
	Description string `json:"description,omitempty"`
}

// HandleListRoles handles GET /api/roles requests
func (h *RolesHandler) HandleListRoles(w ErrorResponseWriter, r *http.Request) {
	log := ctrllog.FromContext(r.Context()).WithName("roles-handler").WithValues("operation", "list")

	roles, err := h.DatabaseService.ListRoles()
	if err != nil {
		log.Error(err, "Failed to list roles")
		w.RespondWithError(errors.NewInternalServerError("Failed to list roles", err))
		return
	}

	log.Info("Successfully listed roles", "count", len(roles))
	data := api.NewResponse(roles, "Successfully listed roles", false)
	RespondWithJSON(w, http.StatusOK, data)
}

// HandleGetRole handles GET /api/roles/{name} requests
func (h *RolesHandler) HandleGetRole(w ErrorResponseWriter, r *http.Request) {
	log := ctrllog.FromContext(r.Context()).WithName("roles-handler").WithValues("operation", "get")

	roleName, err := GetPathParam(r, "name")
	if err != nil {
		w.RespondWithError(errors.NewBadRequestError("Failed to get role name from path", err))
		return
	}
	log = log.WithValues("roleName", roleName)

	role, err := h.DatabaseService.GetRole(roleName)
	if err != nil {
		log.Error(err, "Failed to get role")
		w.RespondWithError(errors.NewNotFoundError("Role not found", err))
		return
	}

	log.Info("Successfully retrieved role")
	data := api.NewResponse(role, "Successfully retrieved role", false)
	RespondWithJSON(w, http.StatusOK, data)
}

// HandleCreateRole handles POST /api/roles requests
func (h *RolesHandler) HandleCreateRole(w ErrorResponseWriter, r *http.Request) {
	log := ctrllog.FromContext(r.Context()).WithName("roles-handler").WithValues("operation", "create")

	var req CreateRoleRequest
	if err := DecodeJSONBody(r, &req); err != nil {
		w.RespondWithError(errors.NewBadRequestError("Invalid request body", err))
		return
	}

	if req.Name == "" {
		w.RespondWithError(errors.NewBadRequestError("Role name is required", nil))
		return
	}

	// Check if role already exists
	existing, _ := h.DatabaseService.GetRole(req.Name)
	if existing != nil {
		w.RespondWithError(errors.NewConflictError("Role '"+req.Name+"' already exists", nil))
		return
	}

	role := &database.Role{
		Name:        req.Name,
		Description: req.Description,
	}

	if err := h.DatabaseService.StoreRole(role); err != nil {
		log.Error(err, "Failed to create role")
		w.RespondWithError(errors.NewInternalServerError("Failed to create role", err))
		return
	}

	log.Info("Successfully created role", "roleName", role.Name)
	data := api.NewResponse(role, "Successfully created role", false)
	RespondWithJSON(w, http.StatusCreated, data)
}

// HandleUpdateRole handles PUT /api/roles/{name} requests
func (h *RolesHandler) HandleUpdateRole(w ErrorResponseWriter, r *http.Request) {
	log := ctrllog.FromContext(r.Context()).WithName("roles-handler").WithValues("operation", "update")

	roleName, err := GetPathParam(r, "name")
	if err != nil {
		w.RespondWithError(errors.NewBadRequestError("Failed to get role name from path", err))
		return
	}
	log = log.WithValues("roleName", roleName)

	// Check if role exists
	existing, err := h.DatabaseService.GetRole(roleName)
	if err != nil {
		log.Error(err, "Failed to get role")
		w.RespondWithError(errors.NewNotFoundError("Role not found", err))
		return
	}

	var req UpdateRoleRequest
	if err := DecodeJSONBody(r, &req); err != nil {
		w.RespondWithError(errors.NewBadRequestError("Invalid request body", err))
		return
	}

	// Only update description (name is immutable since it's the primary key)
	existing.Description = req.Description

	if err := h.DatabaseService.StoreRole(existing); err != nil {
		log.Error(err, "Failed to update role")
		w.RespondWithError(errors.NewInternalServerError("Failed to update role", err))
		return
	}

	log.Info("Successfully updated role")
	data := api.NewResponse(existing, "Successfully updated role", false)
	RespondWithJSON(w, http.StatusOK, data)
}

// HandleDeleteRole handles DELETE /api/roles/{name} requests
// It checks if the role is in use by any agents before allowing deletion
func (h *RolesHandler) HandleDeleteRole(w ErrorResponseWriter, r *http.Request) {
	log := ctrllog.FromContext(r.Context()).WithName("roles-handler").WithValues("operation", "delete")

	roleName, err := GetPathParam(r, "name")
	if err != nil {
		w.RespondWithError(errors.NewBadRequestError("Failed to get role name from path", err))
		return
	}
	log = log.WithValues("roleName", roleName)

	// Check if role exists
	_, err = h.DatabaseService.GetRole(roleName)
	if err != nil {
		log.Error(err, "Failed to get role")
		w.RespondWithError(errors.NewNotFoundError("Role not found", err))
		return
	}

	// Check if any agents use this role
	agentsUsingRole, err := h.getAgentsUsingRole(r, roleName)
	if err != nil {
		log.Error(err, "Failed to check agents using role")
		w.RespondWithError(errors.NewInternalServerError("Failed to check role usage", err))
		return
	}

	if len(agentsUsingRole) > 0 {
		log.Info("Cannot delete role - in use by agents", "agents", agentsUsingRole)
		w.RespondWithError(errors.NewConflictError(
			"Cannot delete role '"+roleName+"' - used by agents: "+formatAgentList(agentsUsingRole),
			nil,
		))
		return
	}

	if err := h.DatabaseService.DeleteRole(roleName); err != nil {
		log.Error(err, "Failed to delete role")
		w.RespondWithError(errors.NewInternalServerError("Failed to delete role", err))
		return
	}

	log.Info("Successfully deleted role")
	data := api.NewResponse(struct{}{}, "Successfully deleted role", false)
	RespondWithJSON(w, http.StatusOK, data)
}

// getAgentsUsingRole returns a list of agent names (namespace/name) that use the given role
func (h *RolesHandler) getAgentsUsingRole(r *http.Request, roleName string) ([]string, error) {
	agentList := &v1alpha2.AgentList{}
	if err := h.KubeClient.List(r.Context(), agentList); err != nil {
		return nil, err
	}

	var agentsUsingRole []string
	for _, agent := range agentList.Items {
		for _, allowedRole := range agent.Spec.AllowedRoles {
			if allowedRole == roleName {
				agentsUsingRole = append(agentsUsingRole, agent.Namespace+"/"+agent.Name)
				break
			}
		}
	}

	return agentsUsingRole, nil
}

// formatAgentList formats a list of agent references for display
func formatAgentList(agents []string) string {
	if len(agents) == 0 {
		return ""
	}
	result := agents[0]
	for i := 1; i < len(agents); i++ {
		result += ", " + agents[i]
	}
	return result
}
