package com.xu.parser.utils;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class ColumnReaderTest {

    @Test
    public void getStringValue() {
        String line ="";
        ColumnReader columnReader = new ColumnReader(line);
        Assert.assertEquals("",columnReader.getStringValue("-"));
    }
}