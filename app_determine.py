import os
import json
from pathlib import Path
from typing import Optional

import polars as pl
from langchain_openai import ChatOpenAI
from pydantic import SecretStr
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from pydantic import BaseModel, Field

import app_config
from lib.files import get_staging_files, read_dataframe


llm_provider_url = os.getenv("OPENAI_API_URL")
llm_provider_key = os.getenv("OPENAI_API_KEY")

def get_chain_lite_rigid(): 
    llm = get_llm(
        model="qwen/qwen3-coder",
        temperature=0,
        top_p=1
    )
    parser = JsonOutputParser(pydantic_object=TableDeterminationResponse)
    prompt = ChatPromptTemplate.from_messages([
        ("system", "You are a robot, tasked with generating rigid structured JSON response"),
        ("human", "{task}")
    ])

    return prompt | llm | parser

def get_chain_medium_rigid(): 
    llm = get_llm(
        model="anthropic/claude-sonnet-4",
        temperature=0,
        top_p=1
    )
    parser = JsonOutputParser(pydantic_object=TableDeterminationResponse)
    prompt = ChatPromptTemplate.from_messages([
        ("system", "You are a robot, tasked with generating rigid structured JSON response"),
        ("human", "{task}")
    ])

    return prompt | llm | parser

def get_chain_heavy_rigid(): 
    llm = get_llm(
        model="anthropic/claude-opus-4.1",
        temperature=0,
        top_p=1
    )
    parser = JsonOutputParser(pydantic_object=TableDeterminationResponse)
    prompt = ChatPromptTemplate.from_messages([
        ("system", "You are a robot, tasked with generating rigid structured JSON response"),
        ("human", "{task}")
    ])

    return prompt | llm | parser

resp_string = "SINGLE STRING RESPONSE: "
class TableDeterminationResponse(BaseModel):
    invoice_line_items: Optional[str] = Field(description=f"{resp_string}Invoice Line-Level Data Table")
    product_master: Optional[str] = Field(description=f"{resp_string}Product Master Level Data, Unique Per Product")
    customer_master: Optional[str] = Field(description=f"{resp_string}Customer Master Level Data, Unique Per Customer")
    uncategorised: list[Optional[str]] = Field(description=f"{resp_string}List of uncategorised tables that fit no other description")
prompt_task_determine_tables: str = """
# YOUR TASK
You are given the names and sample data for multiple data files.
You must determine which filename corresponds to which of the given
purposes stated below.

## invoice_line_items:
    - Should have customer ID, product ID, sell price, sell qty etc.

## product_master:
    - Should have product ID, and multiple fields that provide info at the product master level
    - Should NOT have repetition of same product across different hierarchies, only one row per each

## customer_master
    - Should have customer ID, and multiple fields that provide info at the customer master level
    - Should NOT have repetition of same customer across different hierarchies, only one row per each

## uncategorised
    - JSON list all remaining names that were not selected above

# ADDITIONAL CONTEXT BELOW

"""


class WaterfallDeterminationResponse(BaseModel):
    product_id: Optional[str] = Field(description=f"{resp_string}Product ID")
    customer_id: Optional[str] = Field(description=f"{resp_string}Customer ID")
    country: Optional[str] = Field(description=f"{resp_string}Country")
    quantity: Optional[str] = Field(description=f"{resp_string}Quantity")
    unit_of_measure: Optional[str] = Field(description=f"{resp_string}Unit of Measure")
    gross_price_per_unit: Optional[str] = Field(description=f"{resp_string}Gross Price Per Unit")
    net_price_per_unit: Optional[str] = Field(description=f"{resp_string}Net Price Per Unit")
    gross_price_total: Optional[str] = Field(description=f"{resp_string}Gross Price Total")
    net_price_total: Optional[str] = Field(description=f"{resp_string}Price Total")
prompt_task_determine_waterfall: str = """
# YOUR TASK

Given the tabular data below, you must compose a response of waterfall column
mappings according to details stated below.

# CRITICAL INSTRUCTIONS 
    - Respond only with single strings, no nested JSON
    - Columns must only come from the transactions file
    - Never use the same column name twice
    - No guesswork! Only output the column if confident, do the math
    - Output an empty/null response for any missing columns

Provide only the column name, not a nested JSON.

## product_id:
    - Must denote a unique product
    - Must appear in both the invoice_line_items table, and the product_master table

## customer_id:
    - Must denote a unique customer 
    - Must appear in both the invoice_line_items table, and the customer_master table

## country:
    - Must be full name or short code of country

## quantity:
    - Must be a numeric field that indicates how many units of measure were sold
    - Should usually be an integer, but may be decimal for certain units of volume

## unit_of_measure:
    - A text field that denotes what measure the quantity is in
    - Prioritise column name or contents match with quantity column

## gross_price_per_unit:
    - Must be a numeric field that indicates the gross price paid before any discounts or costs
    - Must be PER UNIT, not multiplied by quantity

## net_price_per_unit:
    - Must be a numeric field that indicates the price paid after discounts, before rebates/cogs etc.
    - Must be PER UNIT, not multiplied by quantity

## gross_price_total:
    - Must be a numeric field that indicates the gross price paid in total, before any discounts or costs
    - Must be extended total, divisible by quantity

## net_price_total:
    - Must be a numeric field that indicates the price paid in total after discounts, before rebates/cogs etc.
    - Must be extended total, divisible by quantity

# ADDITIONAL CONTEXT BELOW

"""


def print_tables_summary(dataframes: list[tuple[str, pl.DataFrame]], n_rows: int = 5) -> str:
    summary: list[str] = []
    for filepath, df in dataframes:
        summary.append(f"\n## Table: {filepath}")
        summary.append(f"Shape: {df.shape[0]} rows, {df.shape[1]} columns")
        summary.append(f"Column Detail: {df.columns}")
        summary.append(f"First {n_rows} rows:")
        summary.append(str(df.head(n_rows)))
    return "\n".join(summary)

def get_llm(
    model: str,
    temperature: float = 0.2,
    top_p: int = 1,
    model_kwargs: dict[str, float | int] = {}
) -> ChatOpenAI:
    return ChatOpenAI(
        base_url=llm_provider_url,
        api_key=SecretStr(llm_provider_key) if llm_provider_key else None,
        model=model,
        temperature=temperature,
        top_p=top_p,
        model_kwargs=model_kwargs,
    )


def determine_tables() -> None:
    output_dir = Path(app_config.get_str(app_config.ConfigKeys.DIR_DATA_OUTPUTS))

    print("Reading staging files")
    staging_files = [
        (filepath, df)
        for filepath in get_staging_files()
        if (df := read_dataframe("Staging", filepath)) is not None
    ]

    print("Writing Table Summary")
    tables_summary = print_tables_summary(staging_files)
    with open(output_dir / "tables_summary.txt", "w") as f:
        f.write(tables_summary)
        f.close()

    print("LLM Determining Tables")
    chain = get_chain_lite_rigid()
    tables_json = chain.invoke({
        "task": prompt_task_determine_tables + "\n" + tables_summary
    })

    # Filter to key tables only
    key_files = [tables_json["invoice_line_items"], tables_json["product_master"], tables_json["customer_master"]]
    key_files = [f for f in key_files if f]  # Remove nulls
    
    filtered_staging_files = [
        (filepath, df) for filepath, df in staging_files 
        if any(key_file in filepath for key_file in key_files)
    ]
    
    filtered_tables_summary = print_tables_summary(filtered_staging_files)

    print("LLM Determining Waterfall")
    chain = get_chain_heavy_rigid()
    waterfall_json = chain.invoke({
        "task": prompt_task_determine_waterfall + "\n" + filtered_tables_summary
    })

    print("Writing JSON Output")
    with open(output_dir / "determinations.json", "w") as f:
        json.dump({
            "tables": tables_json,
            "waterfall": waterfall_json
        }, f, indent=2)


if __name__ == "__main__":
    app_config.init_config("app_config.yaml")
    determine_tables()
