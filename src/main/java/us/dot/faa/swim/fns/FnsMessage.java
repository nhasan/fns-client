package us.dot.faa.swim.fns;

import java.io.StringReader;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Objects;
import java.util.TimeZone;
import java.util.stream.Collectors;

import javax.xml.bind.JAXBException;
import javax.xml.bind.JAXBIntrospector;
import javax.xml.bind.Unmarshaller;

import aero.aixm.event.*;
import org.w3c.dom.Node;

import aero.aixm.extension.fnse.EventExtensionType;
import aero.aixm.message.AIXMBasicMessageType;
import aero.aixm.message.BasicMessageMemberAIXMPropertyType;
import net.opengis.gml.TimePeriodType;
import us.dot.faa.swim.fns.aixm.AixmUtilities;

public class FnsMessage {
    private int fns_id;
    private long correlationId;
    private Timestamp issuedTimestamp;
    private Timestamp updatedTimestamp;
    private Timestamp validFromTimestamp;
    private Timestamp validToTimestamp;
    private boolean validToEstimated;
    private String classification;
    private String locationDesignator;
    private String icaoLocation;
    private String notamAccountability;
    private String notamId;
    private String xoverNotamId;
    private String xoverNotamAccountability;
    private String notamText;
    private String aixmNotamMessage;

    public enum NotamStatus {
        ACTIVE, CANCELLED, EXPIRED
    }

    private NotamStatus status;

    public FnsMessage(final Long correlationId, final String xmlMessage) throws FnsMessageParseException {

        try (StringReader reader = new StringReader(xmlMessage.trim())) {
            final Unmarshaller jaxb_FNSNOTAM_Unmarshaller = AixmUtilities.createAixmUnmarshaller();

            this.correlationId = correlationId;
            this.aixmNotamMessage = xmlMessage;

            AIXMBasicMessageType aixmBasicMessage = (AIXMBasicMessageType) JAXBIntrospector
                    .getValue(jaxb_FNSNOTAM_Unmarshaller.unmarshal(reader));

            this.fns_id = Integer.parseInt(aixmBasicMessage.getId().split("_")[2]);

            for (Node element : aixmBasicMessage.getAny()) {
                BasicMessageMemberAIXMPropertyType basicMessageMemberAIXMPropertyType = jaxb_FNSNOTAM_Unmarshaller
                        .unmarshal(element, BasicMessageMemberAIXMPropertyType.class).getValue();

                String featureTypeName = basicMessageMemberAIXMPropertyType.getAbstractAIXMFeature().getName()
                        .getLocalPart();
                if (Objects.equals(featureTypeName, "Event")) {
                    EventType event = (EventType) basicMessageMemberAIXMPropertyType.getAbstractAIXMFeature()
                            .getValue();
                    EventTimeSliceType eventTimeSlice = event.getTimeSlice().get(0).getEventTimeSlice();
                    List<EventTimeSlicePropertyType> notamEvent = event.getTimeSlice().stream()
                            .filter(evt -> !evt.getEventTimeSlice().getTextNOTAM().isEmpty())
                            .collect(Collectors.toList());

                    if (notamEvent.size() == 1) {
                        TimePeriodType validTime = (TimePeriodType) eventTimeSlice.getValidTime()
                                .getAbstractTimePrimitive().getValue();

                        NOTAMType notam = notamEvent.get(0).getEventTimeSlice().getTextNOTAM().get(0).getNOTAM();
                        EventExtensionType eventExtension = (EventExtensionType) eventTimeSlice.getExtension().get(0)
                                .getAbstractEventExtension().getValue();

                        DateFormat timestampFormater = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                        timestampFormater.setTimeZone(TimeZone.getTimeZone("UTC"));

                        this.issuedTimestamp = new Timestamp(
                                timestampFormater.parse(notam.getIssued().getValue().getValue().toString()).getTime());
                        if (!eventExtension.getLastUpdated().isNil()) {
                            this.updatedTimestamp = new Timestamp(timestampFormater
                                    .parse(eventExtension.getLastUpdated().getValue().getValue().toString()).getTime());
                        }

                        if (validTime.getBeginPosition().getValue().size() != 0) {
                            this.validFromTimestamp = new Timestamp(
                                    timestampFormater.parse(validTime.getBeginPosition().getValue().get(0)).getTime());
                        }

                        var endPosition = validTime.getEndPosition();
                        if (endPosition.getValue().size() != 0) {
                            this.validToTimestamp = new Timestamp(
                                    timestampFormater.parse(endPosition.getValue().get(0)).getTime());
                            if (endPosition.getIndeterminatePosition() != null)
                                this.validToEstimated = endPosition.getIndeterminatePosition().value().equals("unknown");
                        }

                        this.classification = eventExtension.getClassification().getValue();
                        this.notamAccountability = eventExtension.getAccountId().getValue().getValue();
                        this.locationDesignator = notam.getLocation().getValue().getValue();
                        this.notamText = notam.getText().getValue().getValue();

                        if (eventExtension.getIcaoLocation() != null && !eventExtension.getIcaoLocation().isNil())
                            this.icaoLocation = eventExtension.getIcaoLocation().getValue().getValue();

                        var xovernotamElem = eventExtension.getXovernotamID();
                        if (xovernotamElem != null && !xovernotamElem.isNil()) {
                            this.xoverNotamId = xovernotamElem.getValue().getValue();
                        }
                        var xoveraccountElem = eventExtension.getXoveraccountID();
                        if (xoveraccountElem != null && !xoveraccountElem.isNil()) {
                            this.xoverNotamAccountability = xoveraccountElem.getValue().getValue();
                        }

                        var seriesElem = notam.getSeries();
                        String series = (seriesElem != null) ? seriesElem.getValue().getValue() : "";

                        var numberElem = notam.getNumber();
                        if (numberElem != null) {
                            long number = numberElem.getValue().getValue();

                            if (series.startsWith("SW")) {
                                this.notamId = String.format("%s%04d", series, number);
                            } else if (this.classification.equals("INTL") || this.classification.equals("MIL")
                                    || this.classification.equals("LMIL")) {
                                String year = notam.getYear().getValue().getValue();
                                this.notamId = String.format("%s%04d/%s", series, number, year.substring(2));
                            } else if (this.classification.equals("DOM")) {
                                this.notamId = String.format("%02d/%03d",
                                        notam.getIssued().getValue().getValue().getMonth(), number);
                            } else if (this.classification.equals("FDC")) {
                                this.notamId = String.format("%d/%04d",
                                        notam.getIssued().getValue().getValue().getYear() % 10, number);
                            }
                        }
                    }
                }
            }
        } catch (JAXBException | ParseException e) {
            throw new FnsMessageParseException("Failed to create FnsMessage message due to: " + e.getMessage(), e);
        }
    }

    public static class FnsMessageParseException extends Exception
    {
        public FnsMessageParseException(String message, Exception e)
        {
            super(message, e);
        }
    }

    // getters
    public int getFNS_ID() {
        return this.fns_id;
    }

    public long getCorrelationId() {
        return this.correlationId;
    }

    public Timestamp getIssuedTimestamp() {
        return this.issuedTimestamp;
    }

    public Timestamp getUpdatedTimestamp() {
        return this.updatedTimestamp;
    }

    public Timestamp getValidFromTimestamp() {
        return this.validFromTimestamp;
    }

    public Timestamp getValidToTimestamp() {
        return this.validToTimestamp;
    }

    public String getClassification() {
        return this.classification;
    }

    public String getNotamAccountability() {
        return this.notamAccountability;
    }

    public String getNotamText() {
        return this.notamText;
    }

    public String getAixmNotamMessage() {
        return this.aixmNotamMessage;
    }

    public String getLocationDesignator() {
        return this.locationDesignator;
    }

    public NotamStatus getStatus() {
        return this.status;
    }

    public boolean getValidToEstimated() {
        return this.validToEstimated;
    }

    public String getNotamId() {
        return this.notamId;
    }

    public String getXoverNotamId() {
        return this.xoverNotamId;
    }

    public String getXoverNotamAccountability() {
        return this.xoverNotamAccountability;
    }

    public String getIcaoLocation() {
        return this.icaoLocation;
    }

    // setters
    public void setFNS_ID(final int fnsId) {
        this.fns_id = fnsId;
    }

    public void setCorrelationId(final long correlationId) {
        this.correlationId = correlationId;
    }

    public void setIssuedTimestamp(final Timestamp issuedTimestamp) {
        this.issuedTimestamp = issuedTimestamp;
    }

    public void setUpdatedTimestamp(final Timestamp updatedTimestamp) {
        this.updatedTimestamp = updatedTimestamp;
    }

    public void setValidFromTimestamp(final Timestamp validFromTimestamp) {
        this.validFromTimestamp = validFromTimestamp;
    }

    public void setValidToTimestamp(final Timestamp validToTimestamp) {
        this.validToTimestamp = validToTimestamp;
    }

    public void setClassification(final String classification) {
        this.classification = classification;
    }

    public void setNotamAccountability(final String notamAccountability) {
        this.notamAccountability = notamAccountability;
    }

    public void setNotamText(final String notamText) {
        this.notamText = notamText;
    }

    public void setAixmNotamMessage(final String aixmNotamMessage) {
        this.aixmNotamMessage = aixmNotamMessage;
    }

    public void setLocationDesignator(final String locationDesignator) {
        this.locationDesignator = locationDesignator;
    }

    public void setStatus(final NotamStatus status) {
        this.status = status;
    }

    public void setValidToEstimated(boolean validToEstimated) {
        this.validToEstimated = validToEstimated;
    }

    public void setNotamId(final String notamId) {
        this.notamId = notamId;
    }

    public void setXoverNotamId(final String xoverNotamId) {
        this.xoverNotamId = xoverNotamId;
    }

    public void setXoverNotamAccountability(final String xoverNotamAccountability) {
        this.xoverNotamAccountability = xoverNotamAccountability;
    }

    public void setIcaoLocation(final String icaoLocation) {
        this.icaoLocation = icaoLocation;
    }
}
