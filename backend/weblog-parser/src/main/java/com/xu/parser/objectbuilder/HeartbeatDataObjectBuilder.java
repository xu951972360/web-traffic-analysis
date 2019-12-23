package com.xu.parser.objectbuilder;

import com.xu.parser.dataobject.BaseDataObject;
import com.xu.parser.dataobject.HeartbeatDataObject;
import com.xu.parser.utils.ColumnReader;
import com.xu.parser.utils.ParserUtils;
import com.xu.prepaser.PreParsedLog;

import java.util.ArrayList;
import java.util.List;

public class HeartbeatDataObjectBuilder extends AbstractDataObjectBuilder{


    public String getCommand() {
        return "hb";
    }

    public List<BaseDataObject> doBuildDataObjects(PreParsedLog preParsedLog) {
        List<BaseDataObject> dataObjects = new ArrayList<>();
        HeartbeatDataObject dataObject = new HeartbeatDataObject();
        ColumnReader columnReader = new ColumnReader(preParsedLog.getQueryString());
        fillCommonBaseDataObjectValue(dataObject, preParsedLog, columnReader);

        int loadingDuration = 0;
        String plt = columnReader.getStringValue("plt");
        if (!ParserUtils.isNullOrEmptyOrDash(plt)) {
            loadingDuration = Math.round(Float.parseFloat(plt));
        }
        dataObject.setLoadingDuration(loadingDuration);

        int clientPageDuration = 0;
        String psd = columnReader.getStringValue("psd");
        if (!ParserUtils.isNullOrEmptyOrDash(psd)) {
            clientPageDuration = Math.round(Float.parseFloat(psd));
        }
        dataObject.setClientPageDuration(clientPageDuration);

        dataObjects.add(dataObject);
        return dataObjects;
    }
}