package com.silkrode.ai.poa.DTO;

import lombok.Data;

import java.sql.Timestamp;
import java.util.List;

@Data
public class ESInput {
    String es_index;
    List<String> category;
    List<String> keywords;
    List<String> issues;
    List<String> columns;
    List<String> sentiments;
    String raw_keywords;
    Timestamp startTime;
    Timestamp endTime;
    int size;
}
