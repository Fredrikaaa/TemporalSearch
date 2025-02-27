package com.example.query.model;

/**
 * Represents a METADATA expression in the SELECT clause of a query.
 * This column selects document metadata based on the field name.
 * If no field name is specified, all metadata fields are selected.
 */
public class MetadataColumn implements SelectColumn {
    private final String metadataType;
    private final String fieldName;
    private final boolean selectAllFields;
    
    /**
     * Creates a new metadata column for a specific field.
     * 
     * @param metadataType The metadata type (METADATA)
     * @param fieldName The name of the metadata field
     */
    public MetadataColumn(String metadataType, String fieldName) {
        this.metadataType = metadataType;
        this.fieldName = fieldName;
        this.selectAllFields = false;
    }
    
    /**
     * Creates a new metadata column that selects all metadata fields.
     * 
     * @param metadataType The metadata type (METADATA)
     */
    public MetadataColumn(String metadataType) {
        this.metadataType = metadataType;
        this.fieldName = null;
        this.selectAllFields = true;
    }
    
    /**
     * Gets the metadata type.
     * 
     * @return The metadata type
     */
    public String getMetadataType() {
        return metadataType;
    }
    
    /**
     * Gets the field name.
     * 
     * @return The field name, or null if all fields are selected
     */
    public String getFieldName() {
        return fieldName;
    }
    
    /**
     * Indicates whether all metadata fields should be selected.
     * 
     * @return true if all fields should be selected, false for a specific field
     */
    public boolean selectsAllFields() {
        return selectAllFields;
    }
    
    @Override
    public String toString() {
        if (selectAllFields) {
            return metadataType;
        }
        return metadataType + "(\"" + fieldName + "\")";
    }
} 