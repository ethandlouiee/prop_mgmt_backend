from fastapi import FastAPI, Depends, HTTPException, status
from google.cloud import bigquery
from pydantic import BaseModel, Field
from typing import Optional, List, Literal
from datetime import date

app = FastAPI()

PROJECT_ID = "calculatorapi-489215"
DATASET = "property_mgmt"

# --- Pydantic Models ---

# Updated: Amount must be positive (gt=0)
class IncomeCreate(BaseModel):
    amount: float = Field(gt=0)
    date: date
    source: str 

# Updated: Amount must be positive (gt=0)
class ExpenseCreate(BaseModel):
    amount: float = Field(gt=0)
    date: date
    category: str 
    description: Optional[str] = None

# NEW: Model for POST /properties
class PropertyCreate(BaseModel):
    name: str
    tenant_name: Optional[str] = None
    monthly_rent: float = Field(gt=0)

# NEW: Model for POST /transactions
class TransactionCreate(BaseModel):
    property_id: int
    amount: float = Field(gt=0)
    date: date
    transaction_type: Literal["income", "expense"]
    category_or_source: str 
    description: Optional[str] = None

class PropertyUpdate(BaseModel):
    tenant_name: Optional[str] = None
    monthly_rent: Optional[float] = None

# --- Dependency & Helpers ---

def get_bq_client():
    client = bigquery.Client()
    try:
        yield client
    finally:
        client.close()

def verify_property_exists(property_id: int, bq: bigquery.Client):
    query = f"SELECT property_id FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id}"
    results = list(bq.query(query).result())
    if not results:
        raise HTTPException(status_code=404, detail=f"Property {property_id} not found")

# ---------------------------------------------------------------------------
# NEW REQUIREMENTS ADDED
# ---------------------------------------------------------------------------

# Requirement: POST /properties — create a new rental property
@app.post("/properties", status_code=status.HTTP_201_CREATED)
def create_property(prop: PropertyCreate, bq: bigquery.Client = Depends(get_bq_client)):
    rows_to_insert = [{"name": prop.name, "tenant_name": prop.tenant_name, "monthly_rent": prop.monthly_rent}]
    errors = bq.insert_rows_json(f"{PROJECT_ID}.{DATASET}.properties", rows_to_insert)
    if errors:
        raise HTTPException(status_code=500, detail=f"Insert failed: {errors}")
    return {"message": "Property created successfully"}

# Requirement: POST /transactions — record an income or expense transaction
@app.post("/transactions", status_code=status.HTTP_201_CREATED)
def create_transaction(tx: TransactionCreate, bq: bigquery.Client = Depends(get_bq_client)):
    # Requirement: property_id must reference an existing property
    verify_property_exists(tx.property_id, bq)
    
    table = "income" if tx.transaction_type == "income" else "expenses"
    row = {"property_id": tx.property_id, "amount": tx.amount, "date": str(tx.date)}
    
    if tx.transaction_type == "income":
        row["source"] = tx.category_or_source
    else:
        row["category"] = tx.category_or_source
        row["description"] = tx.description

    errors = bq.insert_rows_json(f"{PROJECT_ID}.{DATASET}.{table}", [row])
    if errors:
        raise HTTPException(status_code=500, detail=str(errors))
    return {"message": f"{tx.transaction_type.capitalize()} recorded successfully"}

# ---------------------------------------------------------------------------
# ORIGINAL PROPERTIES ENDPOINTS (STAYED)
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
# ORIGINAL INCOME & EXPENSE ENDPOINTS (STAYED)
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
    if errors: raise HTTPException(status_code=500, detail=str(errors))
    return {"message": "Income record created"}

@app.get("/properties/{property_id}/expenses")
def get_expenses(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    verify_property_exists(property_id, bq)
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.expenses` WHERE property_id = {property_id} ORDER BY date DESC"
    results = bq.query(query).result()
    return [dict(row) for row in results]

@app.post("/properties/{property_id}/expenses", status_code=status.HTTP_201_CREATED)
def create_expense(property_id: int, expense: ExpenseCreate, bq: bigquery.Client = Depends(get_bq_client)):
    verify_property_exists(property_id, bq)
    rows_to_insert = [{"property_id": property_id, "amount": expense.amount, "date": str(expense.date), "category": expense.category, "description": expense.description}]
    errors = bq.insert_rows_json(f"{PROJECT_ID}.{DATASET}.expenses", rows_to_insert)
    if errors: raise HTTPException(status_code=500, detail=str(errors))
    return {"message": "Expense record created"}

# ---------------------------------------------------------------------------
# ORIGINAL ADDITIONAL ENDPOINTS (STAYED)
# ---------------------------------------------------------------------------

@app.get("/properties/{property_id}/summary")
def get_property_summary(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    verify_property_exists(property_id, bq)
    query = f"""
        SELECT 
            (SELECT SUM(amount) FROM `{PROJECT_ID}.{DATASET}.income` WHERE property_id = {property_id}) as total_income,
            (SELECT SUM(amount) FROM `{PROJECT_ID}.{DATASET}.expenses` WHERE property_id = {property_id}) as total_expenses
    """
    row = list(bq.query(query).result())[0]
    income, expenses = row.total_income or 0.0, row.total_expenses or 0.0
    return {"property_id": property_id, "total_income": income, "total_expenses": expenses, "net_cash_flow": income - expenses}

@app.put("/properties/{property_id}")
def update_property(property_id: int, update_data: PropertyUpdate, bq: bigquery.Client = Depends(get_bq_client)):
    verify_property_exists(property_id, bq)
    updates = []
    if update_data.tenant_name is not None: updates.append(f"tenant_name = '{update_data.tenant_name}'")
    if update_data.monthly_rent is not None: updates.append(f"monthly_rent = {update_data.monthly_rent}")
    if not updates: raise HTTPException(status_code=400, detail="No fields provided")
    query = f"UPDATE `{PROJECT_ID}.{DATASET}.properties` SET {', '.join(updates)} WHERE property_id = {property_id}"
    bq.query(query).result()
    return {"message": "Property updated successfully"}

@app.delete("/properties/{property_id}")
def delete_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    verify_property_exists(property_id, bq)
    bq.query(f"DELETE FROM `{PROJECT_ID}.{DATASET}.income` WHERE property_id = {property_id}").result()
    bq.query(f"DELETE FROM `{PROJECT_ID}.{DATASET}.expenses` WHERE property_id = {property_id}").result()
    bq.query(f"DELETE FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id}").result()
    return {"message": f"Property {property_id} deleted"}

@app.get("/reports/overdue")
def get_overdue_rent(bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT p.property_id, p.name, p.tenant_name FROM `{PROJECT_ID}.{DATASET}.properties` p
        WHERE p.property_id NOT IN (
            SELECT property_id FROM `{PROJECT_ID}.{DATASET}.income` 
            WHERE EXTRACT(MONTH FROM date) = EXTRACT(MONTH FROM CURRENT_DATE())
            AND EXTRACT(YEAR FROM date) = EXTRACT(YEAR FROM CURRENT_DATE())
        )
    """
    results = bq.query(query).result()
    return [dict(row) for row in results]