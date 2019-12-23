package com.xu.parser.objectbuilder;

import com.xu.parser.dataobject.BaseDataObject;
import com.xu.parser.dataobject.EventDataObject;
import com.xu.parser.utils.ColumnReader;
import com.xu.parser.utils.ParserUtils;
import com.xu.parser.utils.UrlParseUtils;
import com.xu.prepaser.PreParsedLog;

import java.util.ArrayList;
import java.util.List;

public class EventDataObjectBuilder extends AbstractDataObjectBuilder{


    public String getCommand() {
        return "ev";
    }

    public List<BaseDataObject> doBuildDataObjects(PreParsedLog preParsedLog) {
        List<BaseDataObject> baseDataObjects = new ArrayList<>();
        EventDataObject eventDataObject = new EventDataObject();
        ColumnReader columnReader = new ColumnReader(preParsedLog.getQueryString());
        fillCommonBaseDataObjectValue(eventDataObject, preParsedLog, columnReader);

        eventDataObject.setEventCategory(columnReader.getStringValue("eca"));
        eventDataObject.setEventAction(columnReader.getStringValue("eac"));
        eventDataObject.setEventLabel(columnReader.getStringValue("ela"));
        String eva = columnReader.getStringValue("eva");
        if (!ParserUtils.isNullOrEmptyOrDash(eva)) {
            eventDataObject.setEventValue(Float.parseFloat(eva));
        }
        eventDataObject.setUrl(columnReader.getStringValue("gsurl"));
        eventDataObject.setOriginalUrl(columnReader.getStringValue("gsourl"));
        eventDataObject.setTitle(columnReader.getStringValue("gstl"));
        eventDataObject.setHostDomain(UrlParseUtils.getInfoFromUrl(eventDataObject.getUrl()).getDomain());

        baseDataObjects.add(eventDataObject);
        return baseDataObjects;
    }
}
