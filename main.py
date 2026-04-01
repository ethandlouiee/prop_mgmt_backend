from fastapi import FastAPI, Depends, HTTPException, status
from google.cloud import bigquery
from pydantic import BaseModel, Field
from typing import Optional, List, Literal
from datetime import date
import uuid

app = FastAPI()

PROJECT_ID = "calculatorapi-489215"
DATASET = "property_mgmt"

# --- Schema-Matched Pydantic Models ---

class IncomeCreate(BaseModel):
    amount: float = Field(gt=0)
    date: date
    # Note: Your schema showed 'description' instead of 'source'
    description: Optional[str] = None 

class ExpenseCreate(BaseModel):
    amount: float = Field(gt=0)
    date: date
    category: str 
    vendor: Optional[str] = None # Added based on your screenshot
    description: Optional[str] = None

class PropertyCreate(BaseModel):
    name: str
    address: str
    city: str
    state: str
    postal_code: str
    property_type: str
    tenant_name: Optional[str] = None # Nullable in your schema
    monthly_rent: float = Field(gt=0)

class TransactionCreate(BaseModel):
    property_id: int
    amount: float = Field(gt=0)
    date: date
    transaction_type: Literal["income", "expense"]
    category_or_source: str # Maps to 'category' (expense) or used as description (income)
    vendor: Optional[str] = None
    description: Optional[str] = None

class PropertyUpdate(BaseModel):
    tenant_name: Optional[str] = None
    monthly_rent: Optional[float] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    postal_code: Optional[str] = None
    property_type: Optional[str] = None

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

# --- Endpoints ---

@app.post("/properties", status_code=status.HTTP_201_CREATED)
def create_property(prop: PropertyCreate, bq: bigquery.Client = Depends(get_bq_client)):
    new_id = uuid.uuid4().int >> 96 
    rows_to_insert = [{
        "property_id": new_id, 
        "name": prop.name, 
        "address": prop.address,
        "city": prop.city,
        "state": prop.state,
        "postal_code": prop.postal_code,
        "property_type": prop.property_type,
        "tenant_name": prop.tenant_name, 
        "monthly_rent": prop.monthly_rent
    }]
    errors = bq.insert_rows_json(f"{PROJECT_ID}.{DATASET}.properties", rows_to_insert)
    if errors:
        raise HTTPException(status_code=500, detail=f"Insert failed: {errors}")
    return {"message": "Property created successfully", "property_id": new_id}

@app.post("/transactions", status_code=status.HTTP_201_CREATED)
def create_transaction(tx: TransactionCreate, bq: bigquery.Client = Depends(get_bq_client)):
    verify_property_exists(tx.property_id, bq)
    
    table = "income" if tx.transaction_type == "income" else "expenses"
    id_field = "income_id" if tx.transaction_type == "income" else "expense_id"
    new_id = uuid.uuid4().int >> 96
    
    row = {
        id_field: new_id,
        "property_id": tx.property_id, 
        "amount": tx.amount, 
        "date": str(tx.date)
    }
    
    if tx.transaction_type == "income":
        # Mapping to your income schema: amount, date, description
        row["description"] = tx.category_or_source 
    else:
        # Mapping to your expenses schema: amount, date, category, vendor, description
        row["category"] = tx.category_or_source
        row["vendor"] = tx.vendor
        row["description"] = tx.description

    errors = bq.insert_rows_json(f"{PROJECT_ID}.{DATASET}.{table}", [row])
    if errors:
        raise HTTPException(status_code=500, detail=str(errors))
    return {"message": f"{tx.transaction_type.capitalize()} recorded successfully", "id": new_id}

@app.get("/properties")
def get_properties(bq: bigquery.Client = Depends(get_bq_client)):
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.properties` ORDER BY property_id"
    results = bq.query(query).result()
    return [dict(row) for row in results]

@app.get("/properties/{property_id}")
def get_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id}"
    results = list(bq.query(query).result())
    if not results:
        raise HTTPException(status_code=404, detail="Property not found")
    return dict(results[0])

@app.get("/properties/{property_id}/income")
def get_income(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    verify_property_exists(property_id, bq)
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.income` WHERE property_id = {property_id} ORDER BY date DESC"
    results = bq.query(query).result()
    return [dict(row) for row in results]

@app.post("/properties/{property_id}/income", status_code=status.HTTP_201_CREATED)
def create_income(property_id: int, income: IncomeCreate, bq: bigquery.Client = Depends(get_bq_client)):
    verify_property_exists(property_id, bq)
    new_id = uuid.uuid4().int >> 96
    rows_to_insert = [{
        "income_id": new_id,
        "property_id": property_id, 
        "amount": income.amount, 
        "date": str(income.date), 
        "description": income.description
    }]
    errors = bq.insert_rows_json(f"{PROJECT_ID}.{DATASET}.income", rows_to_insert)
    if errors: raise HTTPException(status_code=500, detail=str(errors))
    return {"message": "Income record created", "income_id": new_id}

@app.get("/properties/{property_id}/expenses")
def get_expenses(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    verify_property_exists(property_id, bq)
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.expenses` WHERE property_id = {property_id} ORDER BY date DESC"
    results = bq.query(query).result()
    return [dict(row) for row in results]

@app.post("/properties/{property_id}/expenses", status_code=status.HTTP_201_CREATED)
def create_expense(property_id: int, expense: ExpenseCreate, bq: bigquery.Client = Depends(get_bq_client)):
    verify_property_exists(property_id, bq)
    new_id = uuid.uuid4().int >> 96
    rows_to_insert = [{
        "expense_id": new_id,
        "property_id": property_id, 
        "amount": expense.amount, 
        "date": str(expense.date), 
        "category": expense.category,
        "vendor": expense.vendor,
        "description": expense.description
    }]
    errors = bq.insert_rows_json(f"{PROJECT_ID}.{DATASET}.expenses", rows_to_insert)
    if errors: raise HTTPException(status_code=500, detail=str(errors))
    return {"message": "Expense record created", "expense_id": new_id}

@app.put("/properties/{property_id}")
def update_property(property_id: int, update_data: PropertyUpdate, bq: bigquery.Client = Depends(get_bq_client)):
    verify_property_exists(property_id, bq)
    updates = []
    data_dict = update_data.dict(exclude_none=True)
    for key, value in data_dict.items():
        if isinstance(value, str):
            updates.append(f"{key} = '{value}'")
        else:
            updates.append(f"{key} = {value}")
            
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