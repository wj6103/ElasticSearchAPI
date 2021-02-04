package com.silkrode.ai.poa.controller;

import com.silkrode.ai.poa.DTO.ESInput;
import com.silkrode.ai.poa.service.ESService;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

@RestController
@RequestMapping("/api/social_media/es/")
public class ESController {

    private final ESService esService;

    public ESController(ESService esService) {
        this.esService = esService;
    }

    @PostMapping("/category")
    public Object category(@RequestBody ESInput esInput) throws Exception {
        return esService.category(esInput);
    }

    @PostMapping("/keywords")
    public Object keywords(@RequestBody ESInput esInput) throws Exception {
        return esService.keywords(esInput);
    }

    @PostMapping("/issues")
    public Object issues(@RequestBody ESInput esInput) throws Exception {
        return esService.issues(esInput);
    }

    @PostMapping("/issue_details")
    public Object issues_details(@RequestBody ESInput esInput) throws Exception{
        return esService.issueDetail(esInput);
    }

    @PostMapping("/news_details")
    public Object news_details(@RequestBody ESInput esInput) throws Exception{
        return esService.newsDetail(esInput);
    }

    @PostMapping("/sentiments")
    public Object sentiments(@RequestBody ESInput esInput) throws Exception{
        return esService.sentiments(esInput);
    }
}
