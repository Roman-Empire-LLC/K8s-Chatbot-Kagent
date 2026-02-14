package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/kagent-dev/kagent/go/internal/httpserver/errors"
	"github.com/kagent-dev/kagent/go/internal/minio"
	"github.com/kagent-dev/kagent/go/pkg/client/api"
	"github.com/kagent-dev/kagent/go/pkg/database"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
)

const metadataFile = ".metadata.json"

// Supported file extensions for RAG document upload
var supportedExtensions = map[string]bool{
	".txt":  true,
	".md":   true,
	".json": true,
	".csv":  true,
	".docx": true,
	".pdf":  true,
}

// RAGIndicesHandler handles RAG index-related requests
type RAGIndicesHandler struct {
	*Base
	MinioClient *minio.Client
}

// NewRAGIndicesHandler creates a new RAGIndicesHandler
func NewRAGIndicesHandler(base *Base, minioClient *minio.Client) *RAGIndicesHandler {
	return &RAGIndicesHandler{Base: base, MinioClient: minioClient}
}

// RAGIndex represents a RAG index stored in MinIO
type RAGIndex struct {
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

// RAGDocument represents a document in a RAG index
type RAGDocument struct {
	Name         string    `json:"name"`
	Size         int64     `json:"size"`
	LastModified time.Time `json:"last_modified"`
	Status       string    `json:"status,omitempty"`
}

// CreateRAGIndexRequest represents the request body for creating a RAG index
type CreateRAGIndexRequest struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// HandleListRAGIndices handles GET /api/indices requests
func (h *RAGIndicesHandler) HandleListRAGIndices(w ErrorResponseWriter, r *http.Request) {
	log := ctrllog.FromContext(r.Context()).WithName("rag-indices-handler").WithValues("operation", "list")

	if h.MinioClient == nil {
		w.RespondWithError(errors.NewInternalServerError("MinIO client not configured", nil))
		return
	}

	// List all buckets
	buckets, err := h.MinioClient.ListBuckets(r.Context())
	if err != nil {
		log.Error(err, "Failed to list buckets")
		w.RespondWithError(errors.NewInternalServerError("Failed to list indices", err))
		return
	}

	// Filter to only RAG indices (buckets with .metadata.json)
	var indices []RAGIndex
	for _, bucket := range buckets {
		metadata, err := h.getIndexMetadata(r.Context(), bucket)
		if err != nil {
			// Not a RAG index, skip
			continue
		}
		indices = append(indices, *metadata)
	}

	log.Info("Successfully listed RAG indices", "count", len(indices))
	data := api.NewResponse(indices, "Successfully listed RAG indices", false)
	RespondWithJSON(w, http.StatusOK, data)
}

// HandleGetRAGIndex handles GET /api/indices/{name} requests
func (h *RAGIndicesHandler) HandleGetRAGIndex(w ErrorResponseWriter, r *http.Request) {
	log := ctrllog.FromContext(r.Context()).WithName("rag-indices-handler").WithValues("operation", "get")

	if h.MinioClient == nil {
		w.RespondWithError(errors.NewInternalServerError("MinIO client not configured", nil))
		return
	}

	indexName, err := GetPathParam(r, "name")
	if err != nil {
		w.RespondWithError(errors.NewBadRequestError("Failed to get index name from path", err))
		return
	}
	log = log.WithValues("indexName", indexName)

	// Check if bucket exists
	exists, err := h.MinioClient.BucketExists(r.Context(), indexName)
	if err != nil {
		log.Error(err, "Failed to check bucket existence")
		w.RespondWithError(errors.NewInternalServerError("Failed to check index", err))
		return
	}
	if !exists {
		w.RespondWithError(errors.NewNotFoundError("RAG index not found", nil))
		return
	}

	// Get metadata
	metadata, err := h.getIndexMetadata(r.Context(), indexName)
	if err != nil {
		log.Error(err, "Failed to get index metadata")
		w.RespondWithError(errors.NewNotFoundError("RAG index not found", err))
		return
	}

	log.Info("Successfully retrieved RAG index")
	data := api.NewResponse(metadata, "Successfully retrieved RAG index", false)
	RespondWithJSON(w, http.StatusOK, data)
}

// HandleCreateRAGIndex handles POST /api/indices requests
func (h *RAGIndicesHandler) HandleCreateRAGIndex(w ErrorResponseWriter, r *http.Request) {
	log := ctrllog.FromContext(r.Context()).WithName("rag-indices-handler").WithValues("operation", "create")

	if h.MinioClient == nil {
		w.RespondWithError(errors.NewInternalServerError("MinIO client not configured", nil))
		return
	}

	var req CreateRAGIndexRequest
	if err := DecodeJSONBody(r, &req); err != nil {
		w.RespondWithError(errors.NewBadRequestError("Invalid request body", err))
		return
	}

	if req.Name == "" {
		w.RespondWithError(errors.NewBadRequestError("Index name is required", nil))
		return
	}

	// Validate name (lowercase, alphanumeric, hyphens only)
	if !isValidIndexName(req.Name) {
		w.RespondWithError(errors.NewBadRequestError("Index name must be lowercase alphanumeric with hyphens only", nil))
		return
	}

	// Check if bucket already exists
	exists, err := h.MinioClient.BucketExists(r.Context(), req.Name)
	if err != nil {
		log.Error(err, "Failed to check bucket existence")
		w.RespondWithError(errors.NewInternalServerError("Failed to check index", err))
		return
	}
	if exists {
		w.RespondWithError(errors.NewConflictError("RAG index '"+req.Name+"' already exists", nil))
		return
	}

	// Create bucket
	if err := h.MinioClient.CreateBucket(r.Context(), req.Name); err != nil {
		log.Error(err, "Failed to create bucket")
		w.RespondWithError(errors.NewInternalServerError("Failed to create index", err))
		return
	}

	// Subscribe bucket to webhook for RAG document processing
	// KAGENT matches the suffix in env vars MINIO_NOTIFY_WEBHOOK_ENABLE_KAGENT
	if err := h.MinioClient.SetBucketNotification(r.Context(), req.Name, "KAGENT"); err != nil {
		log.Error(err, "Failed to set bucket notification")
		// Rollback bucket creation
		_ = h.MinioClient.DeleteBucket(r.Context(), req.Name)
		w.RespondWithError(errors.NewInternalServerError("Failed to configure index notifications", err))
		return
	}

	// Create metadata
	index := RAGIndex{
		Name:        req.Name,
		Description: req.Description,
		CreatedAt:   time.Now().UTC(),
	}

	// Store metadata in bucket
	if err := h.putIndexMetadata(r.Context(), req.Name, &index); err != nil {
		log.Error(err, "Failed to store metadata")
		// Rollback bucket creation
		_ = h.MinioClient.DeleteBucket(r.Context(), req.Name)
		w.RespondWithError(errors.NewInternalServerError("Failed to create index metadata", err))
		return
	}

	log.Info("Successfully created RAG index", "indexName", index.Name)
	data := api.NewResponse(index, "Successfully created RAG index", false)
	RespondWithJSON(w, http.StatusCreated, data)
}

// HandleDeleteRAGIndex handles DELETE /api/indices/{name} requests
func (h *RAGIndicesHandler) HandleDeleteRAGIndex(w ErrorResponseWriter, r *http.Request) {
	log := ctrllog.FromContext(r.Context()).WithName("rag-indices-handler").WithValues("operation", "delete")

	if h.MinioClient == nil {
		w.RespondWithError(errors.NewInternalServerError("MinIO client not configured", nil))
		return
	}

	indexName, err := GetPathParam(r, "name")
	if err != nil {
		w.RespondWithError(errors.NewBadRequestError("Failed to get index name from path", err))
		return
	}
	log = log.WithValues("indexName", indexName)

	// Check if bucket exists
	exists, err := h.MinioClient.BucketExists(r.Context(), indexName)
	if err != nil {
		log.Error(err, "Failed to check bucket existence")
		w.RespondWithError(errors.NewInternalServerError("Failed to check index", err))
		return
	}
	if !exists {
		w.RespondWithError(errors.NewNotFoundError("RAG index not found", nil))
		return
	}

	// Delete bucket (including all objects)
	if err := h.MinioClient.DeleteBucket(r.Context(), indexName); err != nil {
		log.Error(err, "Failed to delete bucket")
		w.RespondWithError(errors.NewInternalServerError("Failed to delete index", err))
		return
	}

	// Delete all document statuses for this index
	if err := h.DatabaseService.DeleteDocumentStatusesForIndex(indexName); err != nil {
		log.Error(err, "Failed to delete document statuses")
		// Don't fail the request, the bucket is already deleted
	}

	log.Info("Successfully deleted RAG index")
	data := api.NewResponse(struct{}{}, "Successfully deleted RAG index", false)
	RespondWithJSON(w, http.StatusOK, data)
}

// UpdateRAGIndexRequest represents the request body for updating a RAG index
type UpdateRAGIndexRequest struct {
	Description string `json:"description"`
}

// HandleUpdateRAGIndex handles PUT /api/indices/{name} requests
func (h *RAGIndicesHandler) HandleUpdateRAGIndex(w ErrorResponseWriter, r *http.Request) {
	log := ctrllog.FromContext(r.Context()).WithName("rag-indices-handler").WithValues("operation", "update")

	if h.MinioClient == nil {
		w.RespondWithError(errors.NewInternalServerError("MinIO client not configured", nil))
		return
	}

	indexName, err := GetPathParam(r, "name")
	if err != nil {
		w.RespondWithError(errors.NewBadRequestError("Failed to get index name from path", err))
		return
	}
	log = log.WithValues("indexName", indexName)

	var req UpdateRAGIndexRequest
	if err := DecodeJSONBody(r, &req); err != nil {
		w.RespondWithError(errors.NewBadRequestError("Invalid request body", err))
		return
	}

	// Get existing metadata
	metadata, err := h.getIndexMetadata(r.Context(), indexName)
	if err != nil {
		log.Error(err, "Failed to get index metadata")
		w.RespondWithError(errors.NewNotFoundError("RAG index not found", err))
		return
	}

	// Update description
	metadata.Description = req.Description

	// Store updated metadata
	if err := h.putIndexMetadata(r.Context(), indexName, metadata); err != nil {
		log.Error(err, "Failed to update metadata")
		w.RespondWithError(errors.NewInternalServerError("Failed to update index", err))
		return
	}

	log.Info("Successfully updated RAG index", "description", req.Description)
	data := api.NewResponse(metadata, "Successfully updated RAG index", false)
	RespondWithJSON(w, http.StatusOK, data)
}

// HandleListDocuments handles GET /api/indices/{name}/documents requests
func (h *RAGIndicesHandler) HandleListDocuments(w ErrorResponseWriter, r *http.Request) {
	log := ctrllog.FromContext(r.Context()).WithName("rag-indices-handler").WithValues("operation", "list-documents")

	if h.MinioClient == nil {
		w.RespondWithError(errors.NewInternalServerError("MinIO client not configured", nil))
		return
	}

	indexName, err := GetPathParam(r, "name")
	if err != nil {
		w.RespondWithError(errors.NewBadRequestError("Failed to get index name from path", err))
		return
	}
	log = log.WithValues("indexName", indexName)

	// Check if bucket exists
	exists, err := h.MinioClient.BucketExists(r.Context(), indexName)
	if err != nil {
		log.Error(err, "Failed to check bucket existence")
		w.RespondWithError(errors.NewInternalServerError("Failed to check index", err))
		return
	}
	if !exists {
		w.RespondWithError(errors.NewNotFoundError("RAG index not found", nil))
		return
	}

	// List objects in bucket
	objects, err := h.MinioClient.ListObjectsInfo(r.Context(), indexName, "")
	if err != nil {
		log.Error(err, "Failed to list objects")
		w.RespondWithError(errors.NewInternalServerError("Failed to list documents", err))
		return
	}

	// Get document statuses from database
	statuses, err := h.DatabaseService.ListDocumentStatusesForIndex(indexName)
	if err != nil {
		log.Error(err, "Failed to list document statuses")
		// Continue without status info
	}

	// Create a map of filename -> status for quick lookup
	statusMap := make(map[string]string)
	for _, s := range statuses {
		statusMap[s.Filename] = s.Status
	}

	// Filter out metadata file and merge with status
	var documents []RAGDocument
	for _, obj := range objects {
		if obj.Name == metadataFile {
			continue
		}
		doc := RAGDocument{
			Name:         obj.Name,
			Size:         obj.Size,
			LastModified: obj.LastModified,
		}
		// Add status if available
		if status, ok := statusMap[obj.Name]; ok {
			doc.Status = status
		}
		documents = append(documents, doc)
	}

	log.Info("Successfully listed documents", "count", len(documents))
	data := api.NewResponse(documents, "Successfully listed documents", false)
	RespondWithJSON(w, http.StatusOK, data)
}

// HandleUploadDocument handles POST /api/indices/{name}/upload requests
func (h *RAGIndicesHandler) HandleUploadDocument(w ErrorResponseWriter, r *http.Request) {
	log := ctrllog.FromContext(r.Context()).WithName("rag-indices-handler").WithValues("operation", "upload")

	if h.MinioClient == nil {
		w.RespondWithError(errors.NewInternalServerError("MinIO client not configured", nil))
		return
	}

	indexName, err := GetPathParam(r, "name")
	if err != nil {
		w.RespondWithError(errors.NewBadRequestError("Failed to get index name from path", err))
		return
	}
	log = log.WithValues("indexName", indexName)

	// Check if bucket exists
	exists, err := h.MinioClient.BucketExists(r.Context(), indexName)
	if err != nil {
		log.Error(err, "Failed to check bucket existence")
		w.RespondWithError(errors.NewInternalServerError("Failed to check index", err))
		return
	}
	if !exists {
		w.RespondWithError(errors.NewNotFoundError("RAG index not found", nil))
		return
	}

	// Parse multipart form (50 MB max)
	if err := r.ParseMultipartForm(50 << 20); err != nil {
		w.RespondWithError(errors.NewBadRequestError("Failed to parse form", err))
		return
	}

	file, handler, err := r.FormFile("file")
	if err != nil {
		w.RespondWithError(errors.NewBadRequestError("Missing file in request", err))
		return
	}
	defer file.Close()

	log = log.WithValues("fileName", handler.Filename, "size", handler.Size)

	// Don't allow overwriting metadata file
	if handler.Filename == metadataFile {
		w.RespondWithError(errors.NewBadRequestError("Cannot upload file with reserved name", nil))
		return
	}

	// Validate file extension
	ext := strings.ToLower(filepath.Ext(handler.Filename))
	if !supportedExtensions[ext] {
		w.RespondWithError(errors.NewBadRequestError(
			fmt.Sprintf("Unsupported file type '%s'. Supported types: .txt, .md, .json, .csv, .docx, .pdf", ext), nil))
		return
	}

	// Upload file to MinIO
	contentType := handler.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	if err := h.MinioClient.UploadFile(r.Context(), indexName, handler.Filename, file, handler.Size, contentType); err != nil {
		log.Error(err, "Failed to upload file to MinIO")
		w.RespondWithError(errors.NewInternalServerError("Failed to upload file", err))
		return
	}

	// Create document status record with pending status
	docStatus := &database.DocumentStatus{
		ID:        uuid.New().String(),
		IndexName: indexName,
		Filename:  handler.Filename,
		Status:    "pending",
	}

	if err := h.DatabaseService.StoreDocumentStatus(docStatus); err != nil {
		log.Error(err, "Failed to store document status")
		// Don't fail the upload, just log the error - the document is already in MinIO
	}

	doc := RAGDocument{
		Name:         handler.Filename,
		Size:         handler.Size,
		LastModified: time.Now().UTC(),
		Status:       "pending",
	}

	log.Info("Successfully uploaded document")
	data := api.NewResponse(doc, "Successfully uploaded document", false)
	RespondWithJSON(w, http.StatusCreated, data)
}

// HandleDeleteDocument handles DELETE /api/indices/{name}/documents/{filename} requests
func (h *RAGIndicesHandler) HandleDeleteDocument(w ErrorResponseWriter, r *http.Request) {
	log := ctrllog.FromContext(r.Context()).WithName("rag-indices-handler").WithValues("operation", "delete-document")

	if h.MinioClient == nil {
		w.RespondWithError(errors.NewInternalServerError("MinIO client not configured", nil))
		return
	}

	indexName, err := GetPathParam(r, "name")
	if err != nil {
		w.RespondWithError(errors.NewBadRequestError("Failed to get index name from path", err))
		return
	}

	filename, err := GetPathParam(r, "filename")
	if err != nil {
		w.RespondWithError(errors.NewBadRequestError("Failed to get filename from path", err))
		return
	}
	log = log.WithValues("indexName", indexName, "filename", filename)

	// Check if bucket exists
	exists, err := h.MinioClient.BucketExists(r.Context(), indexName)
	if err != nil {
		log.Error(err, "Failed to check bucket existence")
		w.RespondWithError(errors.NewInternalServerError("Failed to check index", err))
		return
	}
	if !exists {
		w.RespondWithError(errors.NewNotFoundError("RAG index not found", nil))
		return
	}

	// Don't allow deleting metadata file
	if filename == metadataFile {
		w.RespondWithError(errors.NewBadRequestError("Cannot delete reserved file", nil))
		return
	}

	// Delete file from MinIO
	if err := h.MinioClient.DeleteFile(r.Context(), indexName, filename); err != nil {
		log.Error(err, "Failed to delete file from MinIO")
		w.RespondWithError(errors.NewInternalServerError("Failed to delete document", err))
		return
	}

	// Delete document status from database
	if err := h.DatabaseService.DeleteDocumentStatus(indexName, filename); err != nil {
		log.Error(err, "Failed to delete document status")
		// Don't fail the request, the file is already deleted from MinIO
	}

	log.Info("Successfully deleted document")
	data := api.NewResponse(struct{}{}, "Successfully deleted document", false)
	RespondWithJSON(w, http.StatusOK, data)
}

// HandleDownloadDocument handles GET /api/indices/{name}/documents/{filename} requests
func (h *RAGIndicesHandler) HandleDownloadDocument(w ErrorResponseWriter, r *http.Request) {
	log := ctrllog.FromContext(r.Context()).WithName("rag-indices-handler").WithValues("operation", "download")

	if h.MinioClient == nil {
		w.RespondWithError(errors.NewInternalServerError("MinIO client not configured", nil))
		return
	}

	indexName, err := GetPathParam(r, "name")
	if err != nil {
		w.RespondWithError(errors.NewBadRequestError("Failed to get index name from path", err))
		return
	}

	filename, err := GetPathParam(r, "filename")
	if err != nil {
		w.RespondWithError(errors.NewBadRequestError("Failed to get filename from path", err))
		return
	}
	log = log.WithValues("indexName", indexName, "filename", filename)

	// Check if bucket exists
	exists, err := h.MinioClient.BucketExists(r.Context(), indexName)
	if err != nil {
		log.Error(err, "Failed to check bucket existence")
		w.RespondWithError(errors.NewInternalServerError("Failed to check index", err))
		return
	}
	if !exists {
		w.RespondWithError(errors.NewNotFoundError("RAG index not found", nil))
		return
	}

	// Don't allow downloading metadata file
	if filename == metadataFile {
		w.RespondWithError(errors.NewBadRequestError("Cannot download reserved file", nil))
		return
	}

	// Get the file from MinIO
	data, err := h.MinioClient.GetObject(r.Context(), indexName, filename)
	if err != nil {
		log.Error(err, "Failed to get file from MinIO")
		w.RespondWithError(errors.NewNotFoundError("Document not found", err))
		return
	}

	// Set headers for file download
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filename))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
	w.WriteHeader(http.StatusOK)
	w.Write(data)

	log.Info("Successfully downloaded document")
}

// UpdateStatusRequest represents the request body for updating document status
type UpdateStatusRequest struct {
	Status   string `json:"status"`
	ErrorMsg string `json:"error_msg,omitempty"`
}

// HandleUpdateDocumentStatus handles PUT /api/indices/{name}/documents/{filename}/status requests
func (h *RAGIndicesHandler) HandleUpdateDocumentStatus(w ErrorResponseWriter, r *http.Request) {
	log := ctrllog.FromContext(r.Context()).WithName("rag-indices-handler").WithValues("operation", "update-status")

	indexName, err := GetPathParam(r, "name")
	if err != nil {
		w.RespondWithError(errors.NewBadRequestError("Failed to get index name from path", err))
		return
	}

	filename, err := GetPathParam(r, "filename")
	if err != nil {
		w.RespondWithError(errors.NewBadRequestError("Failed to get filename from path", err))
		return
	}
	log = log.WithValues("indexName", indexName, "filename", filename)

	var req UpdateStatusRequest
	if err := DecodeJSONBody(r, &req); err != nil {
		w.RespondWithError(errors.NewBadRequestError("Invalid request body", err))
		return
	}

	if req.Status == "" {
		w.RespondWithError(errors.NewBadRequestError("Status is required", nil))
		return
	}

	// Update status in database
	if err := h.DatabaseService.UpdateDocumentStatus(indexName, filename, req.Status, req.ErrorMsg); err != nil {
		log.Error(err, "Failed to update document status")
		w.RespondWithError(errors.NewInternalServerError("Failed to update document status", err))
		return
	}

	log.Info("Successfully updated document status", "status", req.Status)
	data := api.NewResponse(struct{}{}, "Successfully updated document status", false)
	RespondWithJSON(w, http.StatusOK, data)
}

// HandleDeleteDocumentStatus handles DELETE /api/indices/{name}/documents/{filename}/status requests
func (h *RAGIndicesHandler) HandleDeleteDocumentStatus(w ErrorResponseWriter, r *http.Request) {
	log := ctrllog.FromContext(r.Context()).WithName("rag-indices-handler").WithValues("operation", "delete-status")

	indexName, err := GetPathParam(r, "name")
	if err != nil {
		w.RespondWithError(errors.NewBadRequestError("Failed to get index name from path", err))
		return
	}

	filename, err := GetPathParam(r, "filename")
	if err != nil {
		w.RespondWithError(errors.NewBadRequestError("Failed to get filename from path", err))
		return
	}
	log = log.WithValues("indexName", indexName, "filename", filename)

	// Delete status from database
	if err := h.DatabaseService.DeleteDocumentStatus(indexName, filename); err != nil {
		log.Error(err, "Failed to delete document status")
		// Don't fail - the status might not exist
	}

	log.Info("Successfully deleted document status")
	data := api.NewResponse(struct{}{}, "Successfully deleted document status", false)
	RespondWithJSON(w, http.StatusOK, data)
}

// getIndexMetadata retrieves the metadata for an index from MinIO
func (h *RAGIndicesHandler) getIndexMetadata(ctx context.Context, bucketName string) (*RAGIndex, error) {
	data, err := h.MinioClient.GetObject(ctx, bucketName, metadataFile)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata: %w", err)
	}

	var index RAGIndex
	if err := json.Unmarshal(data, &index); err != nil {
		return nil, fmt.Errorf("failed to parse metadata: %w", err)
	}

	return &index, nil
}

// putIndexMetadata stores the metadata for an index in MinIO
func (h *RAGIndicesHandler) putIndexMetadata(ctx context.Context, bucketName string, index *RAGIndex) error {
	data, err := json.Marshal(index)
	if err != nil {
		return fmt.Errorf("failed to serialize metadata: %w", err)
	}

	if err := h.MinioClient.PutObjectBytes(ctx, bucketName, metadataFile, data, "application/json"); err != nil {
		return fmt.Errorf("failed to store metadata: %w", err)
	}

	return nil
}

// isValidIndexName validates the index name
func isValidIndexName(name string) bool {
	if len(name) == 0 || len(name) > 63 {
		return false
	}
	// Must start and end with alphanumeric
	if !isAlphanumeric(rune(name[0])) || !isAlphanumeric(rune(name[len(name)-1])) {
		return false
	}
	for _, c := range name {
		if !isAlphanumeric(c) && c != '-' {
			return false
		}
	}
	// Must be lowercase
	return name == strings.ToLower(name)
}

func isAlphanumeric(c rune) bool {
	return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
}
