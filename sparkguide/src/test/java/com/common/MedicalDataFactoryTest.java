package com.common;

import org.junit.Test;

import static org.junit.Assert.*;

public class MedicalDataFactoryTest {

    @Test
    public void createMedicalData() {
        new MedicalDataFactory().createMedicalData(100);
    }
}