package auth

import (
	"context"
	"fmt"
	"strings"

	"github.com/kagent-dev/kagent/go/api/v1alpha2"
	"github.com/kagent-dev/kagent/go/pkg/auth"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type NoopAuthorizer struct{}

func (a *NoopAuthorizer) Check(ctx context.Context, principal auth.Principal, verb auth.Verb, resource auth.Resource) error {
	return nil
}

var _ auth.Authorizer = (*NoopAuthorizer)(nil)

// RBACAuthorizer checks if a user has the required roles to access a resource.
// Roles are defined on Agent resources via the allowedRoles field.
type RBACAuthorizer struct {
	KubeClient client.Client
}

func (a *RBACAuthorizer) Check(ctx context.Context, principal auth.Principal, verb auth.Verb, resource auth.Resource) error {
	fmt.Printf("\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n")
	fmt.Printf("^^^ [RBAC CHECK] type=%s name=%s verb=%s\n", resource.Type, resource.Name, verb)
	fmt.Printf("^^^ [RBAC CHECK] principal.User.ID=%s principal.User.Roles=%v\n", principal.User.ID, principal.User.Roles)
	fmt.Printf("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n\n")

	// Only check Agent resources for now
	if resource.Type != "Agent" {
		fmt.Printf(">>> [RBAC] not Agent type, allowing\n")
		return nil
	}

	// If no resource name specified (e.g., list operation), allow
	// List filtering should be done separately if needed
	if resource.Name == "" {
		fmt.Printf(">>> [RBAC] no resource name, allowing\n")
		return nil
	}

	// Parse namespace/name from resource.Name (format: "namespace/name")
	parts := strings.SplitN(resource.Name, "/", 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid resource name format: %s", resource.Name)
	}
	namespace, name := parts[0], parts[1]

	// Fetch the Agent from Kubernetes
	agent := &v1alpha2.Agent{}
	if err := a.KubeClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, agent); err != nil {
		fmt.Printf(">>> [RBAC] failed to get agent: %v\n", err)
		return fmt.Errorf("failed to get agent: %w", err)
	}

	fmt.Printf(">>> [RBAC] agent.Spec.AllowedRoles=%v\n", agent.Spec.AllowedRoles)

	// If no allowedRoles specified, allow all authenticated users
	if len(agent.Spec.AllowedRoles) == 0 {
		fmt.Printf(">>> [RBAC] no allowedRoles on agent, allowing\n")
		return nil
	}

	// Check if user has any of the allowed roles
	for _, allowedRole := range agent.Spec.AllowedRoles {
		for _, userRole := range principal.User.Roles {
			if allowedRole == userRole {
				fmt.Printf(">>> [RBAC] role match: %s, allowing\n", allowedRole)
				return nil
			}
		}
	}

	fmt.Printf(">>> [RBAC] NO MATCH - DENYING\n")
	return fmt.Errorf("user %s does not have required roles for agent %s/%s (required: %v, has: %v)",
		principal.User.ID, namespace, name, agent.Spec.AllowedRoles, principal.User.Roles)
}

var _ auth.Authorizer = (*RBACAuthorizer)(nil)
