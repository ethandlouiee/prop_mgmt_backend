from fastapi import FastAPI, Depends, HTTPException, status, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from google.cloud import bigquery
from pydantic import BaseModel, Field
from typing import Optional, List, Literal
from datetime import date
import uuid

app = FastAPI()

# Configuration for your specific Google Cloud Project and Dataset
PROJECT_ID = "calculatorapi-489215"
DATASET = "property_mgmt"

# -----------------------------------------------------------------------------
# CUSTOM ERROR HANDLERS
# -----------------------------------------------------------------------------

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """
    Interprets Pydantic validation errors (422) and returns conversational 
    messages for non-technical users.
    """
    details = exc.errors()
    friendly_errors = []
    
    for error in details:
        field = error.get("loc")[-1]
        msg_type = error.get("type")
        
        if msg_type == "missing":
            friendly_errors.append(f"The field '{field}' is required but was left blank.")
        elif msg_type == "greater_than":
            limit = error.get("ctx", {}).get("gt")
            friendly_errors.append(f"The value for '{field}' must be greater than {limit}.")
        elif msg_type == "type_error.integer":
            friendly_errors.append(f"The ID for '{field}' must be a whole number.")
        elif msg_type == "type_error.date":
            friendly_errors.append(f"The date provided for '{field}' is invalid. Please use YYYY-MM-DD.")
        else:
            friendly_errors.append(f"There is an issue with the '{field}' field: {error.get('msg')}")

    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"error": "Invalid Input", "messages": friendly_errors},
    )

@app.exception_handler(HTTPException)
async def custom_http_exception_handler(request: Request, exc: HTTPException):
    """
    Wraps standard HTTP exceptions (like 404s) into a friendly JSON format.
    """
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": "Request Failed", "message": exc.detail},
    )

# -----------------------------------------------------------------------------
# PYDANTIC MODELS (Schema Matched to BigQuery)
# -----------------------------------------------------------------------------

class IncomeCreate(BaseModel):
    amount: float = Field(gt=0)
    date: date
    description: Optional[str] = None 

class ExpenseCreate(BaseModel):
    amount: float = Field(gt=0)
    date: date
    category: str 
    vendor: Optional[str] = None
    description: Optional[str] = None

class PropertyCreate(BaseModel):
    property_id: int = Field(gt=0, description="User-provided unique ID")
    name: str
    address: str
    city: str
    state: str
    postal_code: str
    property_type: str
    tenant_name: Optional[str] = None
    monthly_rent: float = Field(gt=0)

class TransactionCreate(BaseModel):
    property_id: int
    amount: float = Field(gt=0)
    date: date
    transaction_type: Literal["income", "expense"]
    category_or_source: str 
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

# -----------------------------------------------------------------------------
# DEPENDENCIES & HELPERS
# -----------------------------------------------------------------------------

def get_bq_client():
    """Generates a BigQuery client and ensures it is closed after use."""
    client = bigquery.Client()
    try:
        yield client
    finally:
        client.close()

def verify_property_exists(property_id: int, bq: bigquery.Client):
    """Checks if a property exists in BigQuery before performing related operations."""
    query = f"SELECT property_id FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id}"
    results = list(bq.query(query).result())
    if not results:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail=f"We couldn't find a property with ID {property_id}. Please check the ID and try again."
        )

# -----------------------------------------------------------------------------
# PROPERTY ENDPOINTS
# -----------------------------------------------------------------------------

@app.post("/properties", status_code=status.HTTP_201_CREATED)
def create_property(prop: PropertyCreate, bq: bigquery.Client = Depends(get_bq_client)):
    """Creates a new property record using a user-provided ID."""
    check_query = f"SELECT property_id FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {prop.property_id}"
    if list(bq.query(check_query).result()):
        raise HTTPException(
            status_code=400, 
            detail=f"Property ID {prop.property_id} already exists. Please choose a different ID."
        )

    rows_to_insert = [{
        "property_id": prop.property_id,
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
        raise HTTPException(status_code=500, detail=f"Database insertion error: {errors}")
    return {"message": "Property created successfully!", "property_id": prop.property_id}

@app.get("/properties")
def get_properties(bq: bigquery.Client = Depends(get_bq_client)):
    """Returns a complete list of all rental properties."""
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.properties` ORDER BY property_id"
    results = bq.query(query).result()
    return [dict(row) for row in results]

@app.get("/properties/{property_id}")
def get_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """Retrieves full details for a specific property."""
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id}"
    results = list(bq.query(query).result())
    if not results:
        raise HTTPException(status_code=404, detail="No property found with that ID.")
    return dict(results[0])

@app.put("/properties/{property_id}")
def update_property(property_id: int, update_data: PropertyUpdate, bq: bigquery.Client = Depends(get_bq_client)):
    """Updates property fields dynamically based on user input."""
    verify_property_exists(property_id, bq)
    updates = []
    data_dict = update_data.dict(exclude_none=True)
    
    for key, value in data_dict.items():
        if isinstance(value, str):
            updates.append(f"{key} = '{value}'")
        else:
            updates.append(f"{key} = {value}")
            
    if not updates:
        raise HTTPException(status_code=400, detail="No valid update fields provided.")
        
    query = f"UPDATE `{PROJECT_ID}.{DATASET}.properties` SET {', '.join(updates)} WHERE property_id = {property_id}"
    bq.query(query).result()
    return {"message": "Property updated successfully."}

@app.delete("/properties/{property_id}")
def delete_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """Removes a property and all its historical financial records."""
    verify_property_exists(property_id, bq)
    
    # Cleanup child records first to ensure referential integrity
    bq.query(f"DELETE FROM `{PROJECT_ID}.{DATASET}.income` WHERE property_id = {property_id}").result()
    bq.query(f"DELETE FROM `{PROJECT_ID}.{DATASET}.expenses` WHERE property_id = {property_id}").result()
    bq.query(f"DELETE FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id}").result()
    
    return {"message": f"Property {property_id} and all related records deleted."}

# -----------------------------------------------------------------------------
# FINANCIAL ENDPOINTS (Transactions, Income, Expenses)
# -----------------------------------------------------------------------------

@app.post("/transactions", status_code=status.HTTP_201_CREATED)
def create_transaction(tx: TransactionCreate, bq: bigquery.Client = Depends(get_bq_client)):
    """Unified route to record either income or expenses."""
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
        row["description"] = tx.category_or_source 
    else:
        row["category"] = tx.category_or_source
        row["vendor"] = tx.vendor
        row["description"] = tx.description

    errors = bq.insert_rows_json(f"{PROJECT_ID}.{DATASET}.{table}", [row])
    if errors:
        raise HTTPException(status_code=500, detail="Failed to save transaction.")
    return {"message": f"{tx.transaction_type.capitalize()} recorded successfully.", "id": new_id}

@app.get("/properties/{property_id}/income")
def get_income(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """Retrieves all income records for a specific property."""
    verify_property_exists(property_id, bq)
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.income` WHERE property_id = {property_id} ORDER BY date DESC"
    results = bq.query(query).result()
    return [dict(row) for row in results]

@app.post("/properties/{property_id}/income", status_code=status.HTTP_201_CREATED)
def create_income(property_id: int, income: IncomeCreate, bq: bigquery.Client = Depends(get_bq_client)):
    """Direct route for recording property income."""
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
    if errors: raise HTTPException(status_code=500, detail="Failed to record income.")
    return {"message": "Income record created successfully.", "income_id": new_id}

@app.get("/properties/{property_id}/expenses")
def get_expenses(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """Retrieves all expense records for a specific property."""
    verify_property_exists(property_id, bq)
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.expenses` WHERE property_id = {property_id} ORDER BY date DESC"
    results = bq.query(query).result()
    return [dict(row) for row in results]

@app.post("/properties/{property_id}/expenses", status_code=status.HTTP_201_CREATED)
def create_expense(property_id: int, expense: ExpenseCreate, bq: bigquery.Client = Depends(get_bq_client)):
    """Direct route for recording property expenses."""
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
    if errors: raise HTTPException(status_code=500, detail="Failed to record expense.")
    return {"message": "Expense record created successfully.", "expense_id": new_id}

# -----------------------------------------------------------------------------
# REPORTING ENDPOINTS
# -----------------------------------------------------------------------------

@app.get("/reports/overdue")
def get_overdue_rent(bq: bigquery.Client = Depends(get_bq_client)):
    """Identifies properties that have not recorded rent for the current month."""
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