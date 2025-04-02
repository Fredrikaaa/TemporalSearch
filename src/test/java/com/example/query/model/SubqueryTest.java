package com.example.query.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the subquery model classes.
 */
public class SubqueryTest {
    
    @Test
    @DisplayName("Test SubquerySpec creation")
    public void testSubquerySpecCreation() {
        // Create a simple query to use as a subquery
        Query subquery = new Query("test_source");
        
        // Create a subquery specification
        SubquerySpec spec = new SubquerySpec(subquery, "test_alias");
        
        // Verify fields
        assertEquals(subquery, spec.subquery());
        assertEquals("test_alias", spec.alias());
        assertTrue(spec.projectedColumns().isEmpty());
        
        // Create a subquery spec with projected columns
        List<String> columns = List.of("column1", "column2");
        SubquerySpec specWithColumns = new SubquerySpec(subquery, "test_alias", Optional.of(columns));
        
        // Verify fields
        assertEquals(subquery, specWithColumns.subquery());
        assertEquals("test_alias", specWithColumns.alias());
        assertTrue(specWithColumns.projectedColumns().isPresent());
        assertEquals(columns, specWithColumns.projectedColumns().get());
    }
    
    @Test
    @DisplayName("Test SubquerySpec validation")
    public void testSubquerySpecValidation() {
        Query subquery = new Query("test_source");
        
        // Test null subquery
        assertThrows(NullPointerException.class, () -> new SubquerySpec(null, "test_alias"));
        
        // Test null alias
        assertThrows(NullPointerException.class, () -> new SubquerySpec(subquery, null));
        
        // Test empty alias
        assertThrows(IllegalArgumentException.class, () -> new SubquerySpec(subquery, ""));
        
        // Test null projected columns
        assertThrows(NullPointerException.class, () -> new SubquerySpec(subquery, "test_alias", null));
    }
    
    @Test
    @DisplayName("Test JoinCondition creation")
    public void testJoinConditionCreation() {
        // Create a join condition
        JoinCondition condition = new JoinCondition(
            "left_column", 
            "right_column", 
            JoinCondition.JoinType.INNER, 
            TemporalPredicate.INTERSECT
        );
        
        // Verify fields
        assertEquals("left_column", condition.leftColumn());
        assertEquals("right_column", condition.rightColumn());
        assertEquals(JoinCondition.JoinType.INNER, condition.type());
        assertEquals(TemporalPredicate.INTERSECT, condition.temporalPredicate());
        assertTrue(condition.proximityWindow().isEmpty());
        
        // Create a proximity join condition
        JoinCondition proximityCondition = new JoinCondition(
            "left_column", 
            "right_column", 
            JoinCondition.JoinType.INNER, 
            TemporalPredicate.PROXIMITY,
            Optional.of(7)
        );
        
        // Verify fields
        assertEquals(TemporalPredicate.PROXIMITY, proximityCondition.temporalPredicate());
        assertTrue(proximityCondition.proximityWindow().isPresent());
        assertEquals(7, proximityCondition.proximityWindow().get());
    }
    
    @Test
    @DisplayName("Test JoinCondition validation")
    public void testJoinConditionValidation() {
        // Test null left column
        assertThrows(NullPointerException.class, () -> new JoinCondition(
            null, "right_column", JoinCondition.JoinType.INNER, TemporalPredicate.INTERSECT
        ));
        
        // Test null right column
        assertThrows(NullPointerException.class, () -> new JoinCondition(
            "left_column", null, JoinCondition.JoinType.INNER, TemporalPredicate.INTERSECT
        ));
        
        // Test null join type
        assertThrows(NullPointerException.class, () -> new JoinCondition(
            "left_column", "right_column", null, TemporalPredicate.INTERSECT
        ));
        
        // Test null temporal predicate
        assertThrows(NullPointerException.class, () -> new JoinCondition(
            "left_column", "right_column", JoinCondition.JoinType.INNER, null
        ));
        
        // Test PROXIMITY without window
        assertThrows(IllegalArgumentException.class, () -> new JoinCondition(
            "left_column", "right_column", JoinCondition.JoinType.INNER, TemporalPredicate.PROXIMITY, Optional.empty()
        ));
        
        // Test non-PROXIMITY with window
        assertThrows(IllegalArgumentException.class, () -> new JoinCondition(
            "left_column", "right_column", JoinCondition.JoinType.INNER, TemporalPredicate.INTERSECT, Optional.of(7)
        ));
    }
} 