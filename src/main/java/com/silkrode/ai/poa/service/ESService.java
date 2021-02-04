package com.silkrode.ai.poa.service;

import com.silkrode.ai.poa.DTO.ESInput;
import com.silkrode.ai.poa.repository.ESRepository;
import org.springframework.stereotype.Service;



@Service
public class ESService {
    private final ESRepository esRepository;

    public ESService(ESRepository esRepository) {
        this.esRepository = esRepository;
    }

    public Object category(ESInput esInput) throws Exception {
        return esRepository.category(esInput.getEs_index(), esInput.getStartTime(), esInput.getEndTime());
    }

    public Object issues(ESInput esInput) throws Exception {
        String filter = "";
        String category = "{\n" +
                "   \"bool\": {\n" +
                "       \"should\": [\n";
        String keywords = "{\n" +
                "   \"bool\": {\n" +
                "       \"should\": [\n";
        if (esInput.getCategory()!=null) {
            String[] getCategory = esInput.getCategory().toString().replace("[", "").replace("]", "").split(",");
            for (int i = 0; i < getCategory.length; i++) {
                getCategory[i] = "      {\n" +
                        "           \"match_phrase\": {\n" +
                        "               \"category.keyword\": \"" + getCategory[i] + "\"\n          }\n     }";
                category += getCategory[i] + ",\n";
            }
            category = category.substring(0, category.length() - 2);
            category += "\n     ],\n" +
                    "       \"minimum_should_match\": 1\n" +
                    "    }\n" +
                    "}";
            filter += category;
        }
        if (esInput.getKeywords()!=null) {
            String[] getKeywords = esInput.getKeywords().toString().replace("[", "").replace("]", "").split(",");
            for (int i = 0; i < getKeywords.length; i++) {
                getKeywords[i] = "      {\n" +
                        "           \"match_phrase\": {\n" +
                        "               \"keywords_event\": \"" + getKeywords[i] + "\"\n          }\n   }";
                keywords += getKeywords[i] + ",\n";
            }
            keywords = keywords.substring(0, keywords.length() - 2);
            if (filter.length() > 0) {
                keywords += "\n     ],\n" +
                        "            \"minimum_should_match\": 1\n" +
                        "       }\n" +
                        "}";
                filter = filter + ",\n" + keywords;
            }
            else {
                filter = filter + keywords + "\n     ],\n" +
                        "            \"minimum_should_match\": 1\n" +
                        "       }\n" +
                        "}";
            }
        }
        return esRepository.issues(esInput.getEs_index(),esInput.getSize(),filter,esInput.getStartTime(),esInput.getEndTime());
    }

    public Object keywords(ESInput esInput) throws Exception {
        String filter = "";
        String category = "{\n" +
                "   \"bool\": {\n" +
                "       \"should\": [\n";
        String keywords = "{\n" +
                "   \"bool\": {\n" +
                "       \"should\": [\n";
        String issues = "{\n" +
                "   \"bool\": {\n" +
                "       \"should\": [\n";
        String rowKeywords = "{\n" +
                "   \"bool\": {\n" +
                "       \"should\": [\n";
        if (esInput.getCategory()!=null) {
            String[] getCategory = esInput.getCategory().toString().replace("[", "").replace("]", "").split(",");
            for (int i = 0; i < getCategory.length; i++) {
                getCategory[i] = "      {\n" +
                        "           \"match_phrase\": {\n" +
                        "               \"category.keyword\": \"" + getCategory[i] + "\"\n          }\n     }";
                category += getCategory[i] + ",\n";
            }
            category = category.substring(0, category.length() - 2);
            category += "\n     ],\n" +
                    "       \"minimum_should_match\": 1\n" +
                    "    }\n" +
                    "}";
            filter += category;
        }
        if (esInput.getKeywords()!=null) {
            String[] getKeywords = esInput.getKeywords().toString().replace("[", "").replace("]", "").split(",");
            for (int i = 0; i < getKeywords.length; i++) {
                getKeywords[i] = "      {\n" +
                        "           \"match_phrase\": {\n" +
                        "               \"keywords_issue\": \"" + getKeywords[i] + "\"\n          }\n   }";
                keywords += getKeywords[i] + ",\n";
            }
            keywords = keywords.substring(0, keywords.length() - 2);
            if (filter.length() > 0) {
                keywords += "\n     ],\n" +
                        "            \"minimum_should_match\": 1\n" +
                        "       }\n" +
                        "}";
                filter = filter + ",\n" + keywords;
            }
            else {
                filter = filter + keywords + "\n     ],\n" +
                        "            \"minimum_should_match\": 1\n" +
                        "       }\n" +
                        "}";
            }
        }
        if (esInput.getIssues()!=null) {
            String[] getIssues = esInput.getIssues().toString().replace("[", "").replace("]", "").split(",");
            for (int i = 0; i < getIssues.length; i++) {
                getIssues[i] = "        {\n" +
                        "           \"match_phrase\": {\n" +
                        "               \"issue.keyword\": \"" + getIssues[i] + "\"\n          }\n  }";
                issues += getIssues[i] + ",\n";
            }
            issues = issues.substring(0, issues.length() - 2);
            if (filter.length() > 0) {
                issues += "\n   ],\n" +
                        "            \"minimum_should_match\": 1\n" +
                        "   }\n" +
                        "}";
                filter = filter + ",\n" + issues;
            }
            else {
                filter = filter + issues + "\n     ],\n" +
                        "            \"minimum_should_match\": 1\n" +
                        "       }\n" +
                        "}";
            }
        }
        if (esInput.getRaw_keywords()!=null) {
            String[] getRaw_keywords = esInput.getRaw_keywords().split(";");
            for (int i = 0; i < getRaw_keywords.length; i++) {
                getRaw_keywords[i] = "        {\n" +
                        "           \"match_phrase\": {\n" +
                        "               \"issue.keyword\": \"" + getRaw_keywords[i] + "\"\n          }\n  }";
                rowKeywords += getRaw_keywords[i] + ",\n";
            }
            rowKeywords = rowKeywords.substring(0, rowKeywords.length() - 2);
            if (filter.length() > 0) {
                rowKeywords += "\n   ],\n" +
                        "            \"minimum_should_match\": 1\n" +
                        "   }\n" +
                        "}";
                filter = filter + ",\n" + rowKeywords;
            }
            else {
                filter = filter + rowKeywords + "\n     ],\n" +
                        "            \"minimum_should_match\": 1\n" +
                        "       }\n" +
                        "}";
            }
        }
        return esRepository.keywords(esInput.getEs_index(),esInput.getSize(), filter, esInput.getStartTime(), esInput.getEndTime());
    }

    public Object issueDetail(ESInput esInput) throws Exception{
        String filter = "";
        String category = "{\n" +
                "   \"bool\": {\n" +
                "       \"should\": [\n";
        String keywords = "{\n" +
                "   \"bool\": {\n" +
                "       \"should\": [\n";
        String issues = "{\n" +
                "   \"bool\": {\n" +
                "       \"should\": [\n";
        String rowKeywords = "{\n" +
                "   \"bool\": {\n" +
                "       \"should\": [\n";
        if (esInput.getCategory()!=null) {
            String[] getCategory = esInput.getCategory().toString().replace("[", "").replace("]", "").split(",");
            for (int i = 0; i < getCategory.length; i++) {
                getCategory[i] = "      {\n" +
                        "           \"match_phrase\": {\n" +
                        "               \"category.keyword\": \"" + getCategory[i] + "\"\n          }\n     }";
                category += getCategory[i] + ",\n";
            }
            category = category.substring(0, category.length() - 2);
            category += "\n     ],\n" +
                    "       \"minimum_should_match\": 1\n" +
                    "    }\n" +
                    "}";
            filter += category;
        }
        if (esInput.getKeywords()!=null) {
            String[] getKeywords = esInput.getKeywords().toString().replace("[", "").replace("]", "").split(",");
            for (int i = 0; i < getKeywords.length; i++) {
                getKeywords[i] = "      {\n" +
                        "           \"match_phrase\": {\n" +
                        "               \"keywords_event\": \"" + getKeywords[i] + "\"\n          }\n   }";
                keywords += getKeywords[i] + ",\n";
            }
            keywords = keywords.substring(0, keywords.length() - 2);
            if (filter.length() > 0) {
                keywords += "\n     ],\n" +
                        "            \"minimum_should_match\": 1\n" +
                        "       }\n" +
                        "}";
                filter = filter + ",\n" + keywords;
            }
            else {
                filter = filter + keywords + "\n     ],\n" +
                        "            \"minimum_should_match\": 1\n" +
                        "       }\n" +
                        "}";
            }
        }
        if (esInput.getIssues()!=null) {
            String[] getIssues = esInput.getIssues().toString().replace("[", "").replace("]", "").split(",");
            for (int i = 0; i < getIssues.length; i++) {
                getIssues[i] = "        {\n" +
                        "           \"match_phrase\": {\n" +
                        "               \"issue.keyword\": \"" + getIssues[i] + "\"\n          }\n  }";
                issues += getIssues[i] + ",\n";
            }
            issues = issues.substring(0, issues.length() - 2);
            if (filter.length() > 0) {
                issues += "\n   ],\n" +
                        "            \"minimum_should_match\": 1\n" +
                        "   }\n" +
                        "}";
                filter = filter + ",\n" + issues;
            }
            else {
                filter = filter + issues + "\n     ],\n" +
                        "            \"minimum_should_match\": 1\n" +
                        "       }\n" +
                        "}";
            }
        }
        if (esInput.getRaw_keywords()!=null) {
            String[] getRowKeywords = esInput.getRaw_keywords().split(";");
            for (int i = 0; i < getRowKeywords.length; i++) {
                getRowKeywords[i] = "        {\n" +
                        "           \"match_phrase\": {\n" +
                        "               \"content_event\": \"" + getRowKeywords[i] + "\"\n          }\n  }";
                rowKeywords += getRowKeywords[i] + ",\n";
            }
            rowKeywords = rowKeywords.substring(0, rowKeywords.length() - 2);
            if (filter.length() > 0) {
                rowKeywords += "\n   ],\n" +
                        "            \"minimum_should_match\": 1\n" +
                        "   }\n" +
                        "}";
                filter = filter + ",\n" + rowKeywords;
            }
            else {
                filter = filter + rowKeywords + "\n     ],\n" +
                        "            \"minimum_should_match\": 1\n" +
                        "       }\n" +
                        "}";
            }
        }
        return esRepository.issueDetail(esInput.getEs_index(),esInput.getSize(),filter,esInput.getStartTime(),esInput.getEndTime());
    }

    public Object newsDetail(ESInput esInput) throws Exception{
        String filter = "";
        String category = "{\n" +
                "   \"bool\": {\n" +
                "       \"should\": [\n";
        String keywords = "{\n" +
                "   \"bool\": {\n" +
                "       \"should\": [\n";
        String issues = "{\n" +
                "   \"bool\": {\n" +
                "       \"should\": [\n";
        String rowKeywords = "{\n" +
                "   \"bool\": {\n" +
                "       \"should\": [\n";
        if (esInput.getCategory()!=null) {
            String[] getCategory = esInput.getCategory().toString().replace("[", "").replace("]", "").split(",");
            for (int i = 0; i < getCategory.length; i++) {
                getCategory[i] = "      {\n" +
                        "           \"match_phrase\": {\n" +
                        "               \"category.keyword\": \"" + getCategory[i] + "\"\n          }\n     }";
                category += getCategory[i] + ",\n";
            }
            category = category.substring(0, category.length() - 2);
            category += "\n     ],\n" +
                    "       \"minimum_should_match\": 1\n" +
                    "    }\n" +
                    "}";
            filter += category;
        }
        if (esInput.getKeywords()!=null) {
            String[] getKeywords = esInput.getKeywords().toString().replace("[", "").replace("]", "").split(",");
            for (int i = 0; i < getKeywords.length; i++) {
                getKeywords[i] = "      {\n" +
                        "           \"match_phrase\": {\n" +
                        "               \"keywords_event\": \"" + getKeywords[i] + "\"\n          }\n   }";
                keywords += getKeywords[i] + ",\n";
            }
            keywords = keywords.substring(0, keywords.length() - 2);
            if (filter.length() > 0) {
                keywords += "\n     ],\n" +
                        "            \"minimum_should_match\": 1\n" +
                        "       }\n" +
                        "}";
                filter = filter + ",\n" + keywords;
            }
            else {
                filter = filter + keywords + "\n     ],\n" +
                        "            \"minimum_should_match\": 1\n" +
                        "       }\n" +
                        "}";
            }
        }
        if (esInput.getIssues()!=null) {
            String[] getIssues = esInput.getIssues().toString().replace("[", "").replace("]", "").split(",");
            for (int i = 0; i < getIssues.length; i++) {
                getIssues[i] = "        {\n" +
                        "           \"match_phrase\": {\n" +
                        "               \"issue.keyword\": \"" + getIssues[i] + "\"\n          }\n  }";
                issues += getIssues[i] + ",\n";
            }
            issues = issues.substring(0, issues.length() - 2);
            if (filter.length() > 0) {
                issues += "\n   ],\n" +
                        "            \"minimum_should_match\": 1\n" +
                        "   }\n" +
                        "}";
                filter = filter + ",\n" + issues;
            }
            else {
                filter = filter + issues + "\n     ],\n" +
                        "            \"minimum_should_match\": 1\n" +
                        "       }\n" +
                        "}";
            }
        }
        if (esInput.getRaw_keywords()!=null) {
            String[] getRowKeywords = esInput.getRaw_keywords().split(";");
            for (int i = 0; i < getRowKeywords.length; i++) {
                getRowKeywords[i] = "        {\n" +
                        "           \"match_phrase\": {\n" +
                        "               \"content_event\": \"" + getRowKeywords[i] + "\"\n          }\n  }";
                rowKeywords += getRowKeywords[i] + ",\n";
            }
            rowKeywords = rowKeywords.substring(0, rowKeywords.length() - 2);
            if (filter.length() > 0) {
                rowKeywords += "\n   ],\n" +
                        "            \"minimum_should_match\": 1\n" +
                        "   }\n" +
                        "}";
                filter = filter + ",\n" + rowKeywords;
            }
            else {
                filter = filter + rowKeywords + "\n     ],\n" +
                        "            \"minimum_should_match\": 1\n" +
                        "       }\n" +
                        "}";
            }
        }
        return esRepository.newsDetail(esInput.getEs_index(),esInput.getColumns() ,esInput.getSize(),filter,esInput.getStartTime(),esInput.getEndTime());
    }

    public Object sentiments(ESInput esInput) throws Exception{
        String filter = "";
        String category = "{\n" +
                "   \"bool\": {\n" +
                "       \"should\": [\n";
        String keywords = "{\n" +
                "   \"bool\": {\n" +
                "       \"should\": [\n";
        String issues = "{\n" +
                "   \"bool\": {\n" +
                "       \"should\": [\n";
        String rowKeywords = "{\n" +
                "   \"bool\": {\n" +
                "       \"should\": [\n";

        if (esInput.getCategory()!=null) {
            String[] getCategory = esInput.getCategory().toString().replace("[", "").replace("]", "").split(",");
            for (int i = 0; i < getCategory.length; i++) {
                getCategory[i] = "      {\n" +
                        "           \"match_phrase\": {\n" +
                        "               \"category.keyword\": \"" + getCategory[i] + "\"\n          }\n     }";
                category += getCategory[i] + ",\n";
            }
            category = category.substring(0, category.length() - 2);
            category += "\n     ],\n" +
                    "       \"minimum_should_match\": 1\n" +
                    "    }\n" +
                    "}";
            filter += category;
        }
        if (esInput.getKeywords()!=null) {
            String[] getKeywords = esInput.getKeywords().toString().replace("[", "").replace("]", "").split(",");
            for (int i = 0; i < getKeywords.length; i++) {
                getKeywords[i] = "      {\n" +
                        "           \"match_phrase\": {\n" +
                        "               \"keywords_event\": \"" + getKeywords[i] + "\"\n          }\n   }";
                keywords += getKeywords[i] + ",\n";
            }
            keywords = keywords.substring(0, keywords.length() - 2);
            if (filter.length() > 0) {
                keywords += "\n     ],\n" +
                        "            \"minimum_should_match\": 1\n" +
                        "       }\n" +
                        "}";
                filter = filter + ",\n" + keywords;
            }
            else {
                filter = filter + keywords + "\n     ],\n" +
                        "            \"minimum_should_match\": 1\n" +
                        "       }\n" +
                        "}";
            }
        }
        if (esInput.getIssues()!=null) {
            String[] getIssues = esInput.getIssues().toString().replace("[", "").replace("]", "").split(",");
            for (int i = 0; i < getIssues.length; i++) {
                getIssues[i] = "        {\n" +
                        "           \"match_phrase\": {\n" +
                        "               \"issue.keyword\": \"" + getIssues[i] + "\"\n          }\n  }";
                issues += getIssues[i] + ",\n";
            }
            issues = issues.substring(0, issues.length() - 2);
            if (filter.length() > 0) {
                issues += "\n   ],\n" +
                        "            \"minimum_should_match\": 1\n" +
                        "   }\n" +
                        "}";
                filter = filter + ",\n" + issues;
            }
            else {
                filter = filter + issues + "\n     ],\n" +
                        "            \"minimum_should_match\": 1\n" +
                        "       }\n" +
                        "}";
            }
        }
        if (esInput.getRaw_keywords()!=null) {
            String[] getRowKeywords = esInput.getRaw_keywords().split(";");
            for (int i = 0; i < getRowKeywords.length; i++) {
                getRowKeywords[i] = "        {\n" +
                        "           \"match_phrase\": {\n" +
                        "               \"content_event\": \"" + getRowKeywords[i] + "\"\n          }\n  }";
                rowKeywords += getRowKeywords[i] + ",\n";
            }
            rowKeywords = rowKeywords.substring(0, rowKeywords.length() - 2);
            if (filter.length() > 0) {
                rowKeywords += "\n   ],\n" +
                        "            \"minimum_should_match\": 1\n" +
                        "   }\n" +
                        "}";
                filter = filter + ",\n" + rowKeywords;
            }
            else {
                filter = filter + rowKeywords + "\n     ],\n" +
                        "            \"minimum_should_match\": 1\n" +
                        "       }\n" +
                        "}";
            }
        }
        return esRepository.sentiments(esInput.getEs_index(),esInput.getSentiments(),filter,esInput.getStartTime(),esInput.getEndTime());
    }
}
