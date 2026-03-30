from fastapi import FastAPI, Depends, HTTPException, status
from google.cloud import bigquery
from pydantic import BaseModel
from typing import Optional, List
from datetime import date

app = FastAPI()

PROJECT_ID = "calculatorapi-489215"
DATASET = "property_mgmt"

# --- Pydantic Models for Data Validation ---

class IncomeCreate(BaseModel):
    amount: float
    date: date
    source: str  # e.g., "Rent", "Late Fee"

class ExpenseCreate(BaseModel):
    amount: float
    date: date
    category: str # e.g., "Repairs", "Taxes"
    description: Optional[str] = None

class PropertyUpdate(BaseModel):
    tenant_name: Optional[str] = None
    monthly_rent: Optional[float] = None

# --- Dependency: BigQuery client ---

def get_bq_client():
    client = bigquery.Client()
    try:
        yield client
    finally:
        client.close()

# --- Helper Function: Check if Property Exists ---

def verify_property_exists(property_id: int, bq: bigquery.Client):
    query = f"SELECT property_id FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id}"
    results = list(bq.query(query).result())
    if not results:
        raise HTTPException(status_code=404, detail=f"Property {property_id} not found")

# ---------------------------------------------------------------------------
# PROPERTIES ENDPOINTS
# ---------------------------------------------------------------------------

@app.get("/properties")
def get_properties(bq: bigquery.Client = Depends(get_bq_client)):
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.properties` ORDER BY property_id"
    try:
        results = bq.query(query).result()
        return [dict(row) for row in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/properties/{property_id}")
def get_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id}"
    results = list(bq.query(query).result())
    if not results:
        raise HTTPException(status_code=404, detail="Property not found")
    return dict(results[0])

# ---------------------------------------------------------------------------
# INCOME ENDPOINTS
# ---------------------------------------------------------------------------

@app.get("/properties/{property_id}/income")
def get_income(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    verify_property_exists(property_id, bq)
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.income` WHERE property_id = {property_id} ORDER BY date DESC"
    results = bq.query(query).result()
    return [dict(row) for row in results]

@app.post("/properties/{property_id}/income", status_code=status.HTTP_201_CREATED)
def create_income(property_id: int, income: IncomeCreate, bq: bigquery.Client = Depends(get_bq_client)):
    verify_property_exists(property_id, bq)
    rows_to_insert = [{"property_id": property_id, "amount": income.amount, "date": str(income.date), "source": income.source}]
    errors = bq.insert_rows_json(f"{PROJECT_ID}.{DATASET}.income", rows_to_insert)
    if errors:
        raise HTTPException(status_code=500, detail=f"Insert failed: {errors}")
    return {"message": "Income record created"}

# ---------------------------------------------------------------------------
# EXPENSES ENDPOINTS
# ---------------------------------------------------------------------------

@app.get("/properties/{property_id}/expenses")
def get_expenses(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    verify_property_exists(property_id, bq)
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.expenses` WHERE property_id = {property_id} ORDER BY date DESC"
    results = bq.query(query).result()
    return [dict(row) for row in results]

@app.post("/properties/{property_id}/expenses", status_code=status.HTTP_201_CREATED)
def create_expense(property_id: int, expense: ExpenseCreate, bq: bigquery.Client = Depends(get_bq_client)):
    verify_property_exists(property_id, bq)
    rows_to_insert = [{
        "property_id": property_id, "amount": expense.amount, 
        "date": str(expense.date), "category": expense.category, "description": expense.description
    }]
    errors = bq.insert_rows_json(f"{PROJECT_ID}.{DATASET}.expenses", rows_to_insert)
    if errors:
        raise HTTPException(status_code=500, detail=f"Insert failed: {errors}")
    return {"message": "Expense record created"}

# ---------------------------------------------------------------------------
# ADDITIONAL ENDPOINTS
# ---------------------------------------------------------------------------

# 1. Financial Summary for a Property (Calculated Net Flow)
@app.get("/properties/{property_id}/summary")
def get_property_summary(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    verify_property_exists(property_id, bq)
    query = f"""
        SELECT 
            (SELECT SUM(amount) FROM `{PROJECT_ID}.{DATASET}.income` WHERE property_id = {property_id}) as total_income,
            (SELECT SUM(amount) FROM `{PROJECT_ID}.{DATASET}.expenses` WHERE property_id = {property_id}) as total_expenses
    """
    row = list(bq.query(query).result())[0]
    income = row.total_income or 0.0
    expenses = row.total_expenses or 0.0
    return {
        "property_id": property_id,
        "total_income": income,
        "total_expenses": expenses,
        "net_cash_flow": income - expenses
    }

# 2. Update Property Details (e.g., Change Tenant or Rent)
@app.put("/properties/{property_id}")
def update_property(property_id: int, update_data: PropertyUpdate, bq: bigquery.Client = Depends(get_bq_client)):
    verify_property_exists(property_id, bq)
    updates = []
    if update_data.tenant_name is not None:
        updates.append(f"tenant_name = '{update_data.tenant_name}'")
    if update_data.monthly_rent is not None:
        updates.append(f"monthly_rent = {update_data.monthly_rent}")
    
    if not updates:
        raise HTTPException(status_code=400, detail="No fields provided to update")

    query = f"UPDATE `{PROJECT_ID}.{DATASET}.properties` SET {', '.join(updates)} WHERE property_id = {property_id}"
    try:
        bq.query(query).result()
        return {"message": "Property updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# 3. Delete a Property (Cleanup)
@app.delete("/properties/{property_id}")
def delete_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    verify_property_exists(property_id, bq)
    # Note: In a real DB, you'd use foreign key cascades. In BQ, we manually clear associated records.
    try:
        bq.query(f"DELETE FROM `{PROJECT_ID}.{DATASET}.income` WHERE property_id = {property_id}").result()
        bq.query(f"DELETE FROM `{PROJECT_ID}.{DATASET}.expenses` WHERE property_id = {property_id}").result()
        bq.query(f"DELETE FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id}").result()
        return {"message": f"Property {property_id} and all related records deleted"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# 4. Global Report: All Properties with Overdue Income (No payment this month)
@app.get("/reports/overdue")
def get_overdue_rent(bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT p.property_id, p.name, p.tenant_name 
        FROM `{PROJECT_ID}.{DATASET}.properties` p
        WHERE p.property_id NOT IN (
            SELECT property_id FROM `{PROJECT_ID}.{DATASET}.income` 
            WHERE EXTRACT(MONTH FROM date) = EXTRACT(MONTH FROM CURRENT_DATE())
            AND EXTRACT(YEAR FROM date) = EXTRACT(YEAR FROM CURRENT_DATE())
        )
    """
    results = bq.query(query).result()
    return [dict(row) for row in results]
    
