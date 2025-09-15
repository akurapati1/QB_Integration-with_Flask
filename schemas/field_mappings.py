"""
QuickBase to BigQuery field mappings and schemas
Maps QuickBase field IDs to meaningful BigQuery column names
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional

@dataclass
class FieldMapping:
    qb_field_id: str
    bq_column_name: str
    bq_data_type: str
    description: Optional[str] = None

class QuickBaseSchema:
    # Your custom field mappings
    FIELD_MAPPINGS = [
        FieldMapping('1', 'created_date', 'TIMESTAMP', 'Record creation timestamp'),
        FieldMapping('2', 'modified_date', 'TIMESTAMP', 'Record modification timestamp'), 
        FieldMapping('3', 'record_id', 'INTEGER', 'QuickBase record ID'),
        FieldMapping('4', 'created_by', 'JSON', 'User who created the record'),
        FieldMapping('5', 'modified_by', 'JSON', 'User who last modified the record'),
        FieldMapping('6', 'numeric_field_1', 'FLOAT', 'Numeric field 1'),
        FieldMapping('7', 'numeric_field_2', 'FLOAT', 'Numeric field 2'),
        FieldMapping('8', 'notes', 'STRING', 'Additional notes'),
        FieldMapping('9', 'quickbase_url', 'STRING', 'QuickBase record URL'),
        FieldMapping('10', 'first_name', 'STRING', 'First name'),
        FieldMapping('11', 'last_name', 'STRING', 'Last name'),
        FieldMapping('12', 'phone', 'STRING', 'Phone number'),
        FieldMapping('13', 'email', 'STRING', 'Email address'),
        FieldMapping('14', 'job_title', 'STRING', 'Job title'),
        FieldMapping('15', 'full_name', 'STRING', 'Full name'),
        FieldMapping('16', 'company_name', 'STRING', 'Company name'),
        FieldMapping('17', 'additional_field', 'JSON', 'Additional field (complex data)'),
    ]
    
    @classmethod
    def get_field_mapping_dict(cls) -> Dict[str, FieldMapping]:
        return {mapping.qb_field_id: mapping for mapping in cls.FIELD_MAPPINGS}
    
    @classmethod
    def get_bigquery_schema(cls):
        """Generate BigQuery table schema"""
        from google.cloud import bigquery
        
        schema_fields = []
        for mapping in cls.FIELD_MAPPINGS:
            mode = 'NULLABLE'
            
            if mapping.bq_data_type == 'TIMESTAMP':
                field_type = bigquery.SchemaField(mapping.bq_column_name, 'TIMESTAMP', mode=mode, description=mapping.description)
            elif mapping.bq_data_type == 'INTEGER':
                field_type = bigquery.SchemaField(mapping.bq_column_name, 'INTEGER', mode=mode, description=mapping.description)
            elif mapping.bq_data_type == 'FLOAT':
                field_type = bigquery.SchemaField(mapping.bq_column_name, 'FLOAT', mode=mode, description=mapping.description)
            elif mapping.bq_data_type == 'JSON':
                field_type = bigquery.SchemaField(mapping.bq_column_name, 'JSON', mode=mode, description=mapping.description)
            else:  # STRING
                field_type = bigquery.SchemaField(mapping.bq_column_name, 'STRING', mode=mode, description=mapping.description)
            
            schema_fields.append(field_type)
        
        return schema_fields
    
    @classmethod
    def transform_quickbase_record(cls, qb_record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform QuickBase record to BigQuery format"""
        mapping_dict = cls.get_field_mapping_dict()
        transformed_record = {}
        
        for qb_field_id, field_data in qb_record.items():
            if qb_field_id in mapping_dict:
                mapping = mapping_dict[qb_field_id]
                column_name = mapping.bq_column_name
                raw_value = field_data.get('value')
                
                # Transform the value based on data type
                if mapping.bq_data_type == 'TIMESTAMP' and raw_value:
                    transformed_record[column_name] = raw_value
                elif mapping.bq_data_type == 'JSON' and raw_value:
                    transformed_record[column_name] = raw_value
                elif mapping.bq_data_type in ['INTEGER', 'FLOAT'] and raw_value is not None:
                    try:
                        transformed_record[column_name] = float(raw_value) if mapping.bq_data_type == 'FLOAT' else int(raw_value)
                    except (ValueError, TypeError):
                        transformed_record[column_name] = None
                else:
                    transformed_record[column_name] = raw_value
        
        return transformed_record