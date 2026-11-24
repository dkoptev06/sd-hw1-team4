package order

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

type fakeOrders struct {
	start  StartResult
	view   View
	finish FinishReceipt
	err    error
}

func (f *fakeOrders) Start(context.Context, uuid.UUID) (StartResult, error) {
	return f.start, f.err
}
func (f *fakeOrders) Get(context.Context, uuid.UUID) (View, error) { return f.view, f.err }
func (f *fakeOrders) Finish(context.Context, uuid.UUID) (FinishReceipt, error) {
	return f.finish, f.err
}

func serve(t *testing.T, orders orderOperations, method, path, body string) (int, map[string]any) {
	t.Helper()
	router := echo.New()
	(&orderHandler{orders: orders}).registerRoutes(router)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
	var decoded map[string]any
	if rec.Body.Len() > 0 {
		if err := json.Unmarshal(rec.Body.Bytes(), &decoded); err != nil {
			t.Fatalf("body %q: %v", rec.Body.String(), err)
		}
	}
	return rec.Code, decoded
}

func TestStartOrder(t *testing.T) {
	orders := &fakeOrders{start: StartResult{OrderID: uuid.New(), Status: Starting}}
	path, body := "/orders", `{"offer_id":"`+uuid.NewString()+`"}`
	if status, got := serve(t, orders, "POST", path, body); status != http.StatusAccepted || got["status"] != "STARTING" {
		t.Fatalf("%d %v", status, got)
	}
	orders.start.Replayed, orders.start.Status = true, Active
	if status, got := serve(t, orders, "POST", path, body); status != http.StatusOK || got["status"] != "ACTIVE" {
		t.Fatalf("replay %d %v", status, got)
	}
}

func TestGetOrderNullFields(t *testing.T) {
	orders := &fakeOrders{view: View{ID: uuid.New(), Status: Starting}}
	status, got := serve(t, orders, "GET", "/orders/"+uuid.NewString(), "")
	if status != http.StatusOK || got["status"] != "STARTING" {
		t.Fatalf("%d %v", status, got)
	}
	for _, field := range []string{"cost", "started_at", "finish_time"} {
		if value, present := got[field]; !present || value != nil {
			t.Fatalf("%s must be null, got %v (present=%t)", field, value, present)
		}
	}
}
