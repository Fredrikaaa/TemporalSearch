package com.example.query.binding;

import com.example.core.Position;

import java.time.LocalDate;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a single detailed match from executing a query condition.
 * Includes the matched value, its type, position, and potentially variable binding info.
 * 
 * TODO: Consider refactoring join results into a separate class (e.g., JoinMatchPair) 
 *       instead of overloading MatchDetail, for better separation of concerns if 
 *       join logic becomes more complex.
 */
public record MatchDetail(
    Object value,
    ValueType valueType,
    Position position,
    String conditionId, // Identifier for the condition that produced this match
    String variableName, // Optional variable name if this match binds a variable
    // Fields for join results - Left side implicitly uses main fields (value, position, etc.)
    Optional<Integer> rightDocumentId, 
    Optional<Integer> rightSentenceId,
    Optional<Object> rightValue,         // Value from the right side of the join
    Optional<ValueType> rightValueType,  // ValueType from the right side
    Optional<String> rightVariableName // Variable name from the right side (if applicable)
) {
    /** Canonical constructor */
    public MatchDetail {
        Objects.requireNonNull(valueType, "valueType cannot be null");
        Objects.requireNonNull(position, "position cannot be null");
        Objects.requireNonNull(conditionId, "conditionId cannot be null");
        Objects.requireNonNull(rightDocumentId, "rightDocumentId cannot be null");
        Objects.requireNonNull(rightSentenceId, "rightSentenceId cannot be null");
        Objects.requireNonNull(rightValue, "rightValue cannot be null");
        Objects.requireNonNull(rightValueType, "rightValueType cannot be null");
        Objects.requireNonNull(rightVariableName, "rightVariableName cannot be null");
    }
    
    // Constructor for non-join matches
    public MatchDetail(Object value, ValueType valueType, Position position, String conditionId, String variableName) {
        this(value, valueType, position, conditionId, variableName, 
             Optional.empty(), Optional.empty(), 
             Optional.empty(), Optional.empty(), Optional.empty());
    }
    
    // Convenience constructor for join results
    public MatchDetail(MatchDetail left, MatchDetail right) {
        this(left.value(),                     // value from left
             left.valueType(),                 // valueType from left
             left.position(),                  // position from left
             left.conditionId(),               // conditionId from left
             left.variableName(),              // variableName from left
             Optional.of(right.getDocumentId()), // rightDocumentId from right's getter
             right.getSentenceId() != -1 ? Optional.of(right.getSentenceId()) : Optional.empty(), // rightSentenceId from right's getter, check for -1
             Optional.ofNullable(right.value()),           // rightValue from right
             Optional.ofNullable(right.valueType()),       // rightValueType from right
             Optional.ofNullable(right.variableName())     // rightVariableName from right
            );
    }
    
    // --- Convenience Getters for Position fields --- 
    
    /** Document ID */
    public int getDocumentId() { return position.getDocumentId(); }
    
    /** Sentence ID (-1 if not applicable) */
    public int getSentenceId() { return position.getSentenceId(); }
    
    /** Start position */
    public int getStartPosition() { return position.getBeginPosition(); }
    
    /** End position */
    public int getEndPosition() { return position.getEndPosition(); }
    
    /** Document Date */
    public LocalDate getDocumentDate() { return position.getTimestamp(); }
    
    // --- Convenience Getters for Join fields ---
    /** Optional Right Document ID */
    public Optional<Integer> getRightDocumentId() { return rightDocumentId; }
    
    /** Optional Right Sentence ID */
    public Optional<Integer> getRightSentenceId() { return rightSentenceId; }
    
    /** Optional Right Value */
    public Optional<Object> getRightValue() { return rightValue; }
    
    /** Optional Right Value Type */
    public Optional<ValueType> getRightValueType() { return rightValueType; }
    
    /** Optional Right Variable Name */
    public Optional<String> getRightVariableName() { return rightVariableName; }
    
    // --- Helper Methods --- 

    /** Check if this detail represents a variable binding */
    public boolean isVariableBinding() { return variableName != null; }

    /** Check if this detail resulted from a join operation */
    public boolean isJoinResult() { return rightDocumentId.isPresent(); }
    
    @Override
    public String toString() {
        // Simple toString, more detail might be needed for debugging
        return "MatchDetail{" +
               "value=" + value +
               ", type=" + valueType +
               ", pos=" + position +
               (variableName != null ? ", var='" + variableName + '\'' : "") +
               (isJoinResult() ? ", rightDocId=" + rightDocumentId.get() : "") +
               (isJoinResult() && rightSentenceId.isPresent() ? ", rightSentId=" + rightSentenceId.get() : "") +
               "}";
    }

    /**
     * Gets the matched value interpreted as a LocalDate, if applicable.
     * Returns null if the valueType is not DATE or the value is not a LocalDate.
     * @return The matched LocalDate, or null.
     */
    public LocalDate getMatchedDate() {
        if (valueType == ValueType.DATE && value instanceof LocalDate dateValue) {
            return dateValue;
        }
        return null;
    }
    
    /**
     * Gets the text span covered by the position.
     * @return The text span.
     */
    public String getTextSpan() {
        // Assuming Position has a method or way to get the text span
        // This might need access to the original document content, 
        // which is not directly stored here. 
        // Placeholder: return position info. A real implementation might need more context.
        return "Span[" + getStartPosition() + ":" + getEndPosition() + "]"; 
    }
} 