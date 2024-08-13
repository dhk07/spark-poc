package com.spark.poc.service.controller;

import com.spark.poc.service.config.Utils;
import com.spark.poc.service.practice.RDDExample;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.net.ssl.SSLEngineResult;

@RestController
@RequestMapping("/spark")
public class SparkController {
    @Autowired
    private RDDExample rddExample;

    @GetMapping("/rdd")
    public ResponseEntity<?> rddExample(){
        try {
            rddExample.sumOfList();
            return new ResponseEntity<>("", HttpStatus.OK);
        }catch (Exception e){
            e.printStackTrace();
            return new ResponseEntity<>(e.getMessage(), HttpStatus.EXPECTATION_FAILED);
        }

    }

}
