package com.gs.cloud.warehouse.ics.function.process;

import com.gs.cloud.warehouse.ics.entity.Incident;
import com.gs.cloud.warehouse.ics.entity.TxCard;
import com.gs.cloud.warehouse.ics.entity.TxCardTriggerIncident;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class TxCardTriggerIncidentFunction extends RichCoGroupFunction<TxCard, Incident, TxCardTriggerIncident>{
    private transient ValueState<TxCard> lastTxCardValueState;
    @Override
    public void open(Configuration parameters) throws Exception {
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.hours(36))
                .setUpdateType(StateTtlConfig.UpdateType.Disabled)
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                .build();
        ValueStateDescriptor<TxCard> descriptor =
                new ValueStateDescriptor<>(
                        "lastTxCard",
                        TypeInformation.of(TxCard.class));
        descriptor.enableTimeToLive(ttlConfig);
        lastTxCardValueState = getRuntimeContext().getState(descriptor);
        super.open(parameters);
    }

    @Override
    public void coGroup(Iterable<TxCard> first, Iterable<Incident> second, Collector<TxCardTriggerIncident> out) throws Exception {
        List<TxCard> txCardList = (List<TxCard>) first;
        List<Incident> incidentList = (List<Incident>) second;
        if ((txCardList == null || txCardList.isEmpty())&&(incidentList == null || incidentList.isEmpty())) {
            return;
        }
        //定位上报中断
        boolean isLocRepInterr = false;
        if(incidentList != null && !incidentList.isEmpty()) {
            for (Incident incident : incidentList) {
                if(incident.getIncidId().equals("DW001")) {
                    out.collect(createIncidentTriggerIncident(incident, "DW001", "工牌定位上报中断"));
                    isLocRepInterr = true;
                    break;
                }
            }
        }

        if (txCardList != null && !txCardList.isEmpty()) {
            Collections.sort(txCardList);
            //上报中断恢复
            if (isLocRepInterr) {
                out.collect(createTxCardTriggerIncident(txCardList.get(0), "", "工牌定位上报中断恢复"));
            }
            TxCard lastTxCard = lastTxCardValueState.value();
            for (TxCard txCard : txCardList) {
                //System.out.println(txCard);
                Long packetNumber = txCard.getPacketNumber();
                Double electricity = txCard.getElectricity();
                Long signalStrength = txCard.getSignalStrength();
                String beaconList = txCard.getBeaconList();
                Long flagStop = txCard.getFlagStop();
                //包序列号
                if (packetNumber != null
                        && lastTxCard != null
                        && lastTxCard.getPacketNumber() != null
                        && lastTxCard.getRepTimeT8().getTime() <= txCard.getRepTimeT8().getTime()
                        && packetNumber - lastTxCard.getPacketNumber() >= 6) {
                    out.collect(createTxCardTriggerIncident(txCard, String.valueOf(packetNumber), "工牌定位漏报"));
                }

                //电池低电量
                if (electricity != null) {
                    if ((lastTxCard == null && electricity <= 20)
                            || (lastTxCard != null
                            && lastTxCard.getElectricity() != null
                            && lastTxCard.getRepTimeT8().getTime() <= txCard.getRepTimeT8().getTime()
                            && lastTxCard.getElectricity() > 20
                            && electricity <= 20)) {
                        out.collect(createTxCardTriggerIncident(txCard, String.valueOf(electricity), "工牌低电量", "20", lastTxCard == null?"":String.valueOf(lastTxCard.getElectricity())));
                    }
                }
                //信号强度
                if (signalStrength != null) {
                    if ((lastTxCard == null && signalStrength > 100)
                            || (lastTxCard != null
                            && lastTxCard.getSignalStrength() != null
                            && lastTxCard.getRepTimeT8().getTime() <= txCard.getRepTimeT8().getTime()
                            && lastTxCard.getSignalStrength() <= 100
                            && signalStrength > 100)) {
                        out.collect(createTxCardTriggerIncident(txCard, String.valueOf(signalStrength), "工牌网络信号差"));
                    }
                }

                //工牌充电
                if (electricity != null
                    && lastTxCard != null
                    && lastTxCard.getElectricity() != null) {
                        if ((lastTxCard.getPreElectricity() == null && electricity > lastTxCard.getElectricity())
                               || (lastTxCard.getPreElectricity() != null
                                && lastTxCard.getRepTimeT8().getTime() <= txCard.getRepTimeT8().getTime()
                                && lastTxCard.getPreElectricity() >= lastTxCard.getElectricity()
                                && electricity > lastTxCard.getElectricity())) {
                            out.collect(createTxCardTriggerIncident(txCard, String.valueOf(electricity), "工牌充电"));
                            txCard.setPreElectricity(lastTxCard.getElectricity());
                        } else {
                            txCard.setPreElectricity(lastTxCard.getElectricity());
                        }
                }

                //电池充满
                if (electricity != null
                        && lastTxCard != null
                        && lastTxCard.getElectricity() != null
                        && lastTxCard.getRepTimeT8().getTime() <= txCard.getRepTimeT8().getTime()
                        && lastTxCard.getElectricity() < 80
                        && electricity >= 80){
                    out.collect(createTxCardTriggerIncident(txCard, String.valueOf(electricity), "工牌电量充满"));
                }

                //工牌停止运动标志(之前的逻辑是累计6次告警,已废弃)
//                if (flagStop != null) {
//                    if (lastTxCard != null) {
//                        if (flagStop == 0) {
//                            txCard.setMoveCount(lastTxCard.getMoveCount() + 1);
//                            txCard.setStopCount(0L);
//                            txCard.setMove(lastTxCard.getMove());
//                        } else {
//                            txCard.setStopCount(lastTxCard.getStopCount() + 1);
//                            txCard.setMoveCount(0L);
//                            txCard.setMove(lastTxCard.getMove());
//                        }
//                        if (txCard.getMoveCount() == 6
//                                && (lastTxCard.getMove() == null || lastTxCard.getMove() == false)) {
//                            out.collect(createTxCardTriggerIncident(txCard, String.valueOf(flagStop), "工牌静止恢复"));
//                            txCard.setMove(true);
//                        }
//                        if (txCard.getStopCount() == 6
//                                && (lastTxCard.getMove() == null || lastTxCard.getMove() == true)) {
//                            out.collect(createTxCardTriggerIncident(txCard, String.valueOf(flagStop), "工牌静止"));
//                            txCard.setMove(false);
//                        }
//                    } else {
//                        if(flagStop == 1){
//                            txCard.setStopCount(1L);
//                            txCard.setMoveCount(0L);
//                        } else {
//                            txCard.setMoveCount(1L);
//                            txCard.setStopCount(0L);
//                        }
//                    }
//                } else {
//                    txCard.setStopCount(0L);
//                    txCard.setMoveCount(0L);
//                }

                //工牌停止运动标志(新的逻辑采用报相同的标志位超过一分钟，则触发告警)
                if (flagStop != null) {
                    if (lastTxCard != null) {
                        if (flagStop == 0) {
                            txCard.setMoveCount(lastTxCard.getMoveCount() + 1);
                            txCard.setStopCount(0L);
                            txCard.setStopOrMoveTime((lastTxCard.getStopOrMoveTime() == null||flagStop!=lastTxCard.getFlagStop())?txCard.getRepTimeT8():lastTxCard.getStopOrMoveTime());
                        } else {
                            txCard.setStopCount(lastTxCard.getStopCount() + 1);
                            txCard.setMoveCount(0L);
                            txCard.setStopOrMoveTime((lastTxCard.getStopOrMoveTime() == null||flagStop!=lastTxCard.getFlagStop())?txCard.getRepTimeT8():lastTxCard.getStopOrMoveTime());
                        }

                        if (txCard.getMoveCount() > 0
                                && txCard.getRepTimeT8().getTime() > txCard.getStopOrMoveTime().getTime()
                                && txCard.getRepTimeT8().getTime() - txCard.getStopOrMoveTime().getTime() >= 60000) {
                            //System.out.println(createTxCardTriggerIncident(txCard, String.valueOf(flagStop), "工牌静止恢复"));
                            out.collect(createTxCardTriggerIncident(txCard, String.valueOf(flagStop), "工牌静止恢复"));
                            txCard.setMoveCount(0L);
                            txCard.setStopOrMoveTime(null);
                        }
                        if (txCard.getStopCount() > 0
                                && txCard.getRepTimeT8().getTime() > txCard.getStopOrMoveTime().getTime()
                                && txCard.getRepTimeT8().getTime() - txCard.getStopOrMoveTime().getTime() >= 60000) {
                            //System.out.println(createTxCardTriggerIncident(txCard, String.valueOf(flagStop), "工牌静止"));
                            out.collect(createTxCardTriggerIncident(txCard, String.valueOf(flagStop), "工牌静止"));
                            txCard.setStopCount(0L);
                            txCard.setStopOrMoveTime(null);
                        }
                    } else {
                        if(flagStop == 1){
                            txCard.setStopCount(1L);
                            txCard.setMoveCount(0L);
                            txCard.setStopOrMoveTime(txCard.getRepTimeT8());
                        } else {
                            txCard.setMoveCount(1L);
                            txCard.setStopCount(0L);
                            txCard.setStopOrMoveTime(txCard.getRepTimeT8());
                        }
                    }
                } else {
                    txCard.setStopCount(0L);
                    txCard.setMoveCount(0L);
                    txCard.setStopOrMoveTime(null);
                }

                //定位beacon
                if (beaconList != null && !beaconList.equals("")) {
                    if (lastTxCard != null) {
                        if (!beaconList.equals("[]")) {
                            txCard.setBeaconListNotNullCount(lastTxCard.getBeaconListNotNullCount() + 1);
                            txCard.setBeaconListNullCount(0L);
                            txCard.setLocNormal(lastTxCard.getLocNormal());
                        } else {
                            txCard.setBeaconListNullCount(lastTxCard.getBeaconListNullCount() + 1);
                            txCard.setBeaconListNotNullCount(0L);
                            txCard.setLocNormal(lastTxCard.getLocNormal());
                        }
                        if (txCard.getBeaconListNullCount() == 6
                                && (lastTxCard.getLocNormal() == null || lastTxCard.getLocNormal() == true)) {
                            out.collect(createTxCardTriggerIncident(txCard, beaconList, "工牌无法定位"));
                            txCard.setLocNormal(false);
                        }
                        if (txCard.getBeaconListNotNullCount() == 6
                                && (lastTxCard.getLocNormal() == null || lastTxCard.getLocNormal() == false)) {
                            out.collect(createTxCardTriggerIncident(txCard, beaconList, "工牌定位恢复"));
                            txCard.setLocNormal(true);
                        }
                    } else {
                        if (!beaconList.equals("[]")) {
                            txCard.setBeaconListNotNullCount(1L);
                            txCard.setBeaconListNullCount(0L);
                        } else {
                            txCard.setBeaconListNullCount(1L);
                            txCard.setBeaconListNotNullCount(0L);
                        }
                    }
                } else {
                    txCard.setBeaconListNullCount(0L);
                    txCard.setBeaconListNotNullCount(0L);
                }
                lastTxCard = txCard;
            }
            lastTxCardValueState.update(lastTxCard);
        }
    }

    private TxCardTriggerIncident createIncidentTriggerIncident(Incident incident, String incidentValue, String incidentName){
        TxCardTriggerIncident txCardTrigIncid = new TxCardTriggerIncident();
        txCardTrigIncid.setCardSn(incident.getCardSn());
        txCardTrigIncid.setItemId(incident.getItemId());
        txCardTrigIncid.setItemName(incident.getItemName());
        txCardTrigIncid.setCrewId(incident.getCrewId());
        txCardTrigIncid.setCrewName(incident.getCrewName());
        txCardTrigIncid.setCloudTime(incident.getIncidTimeT8());
        txCardTrigIncid.setReportTime(null);
        txCardTrigIncid.setTriggerIncidentName(incidentName);
        txCardTrigIncid.setTriggerIncidentValue(incidentValue);
        return txCardTrigIncid;
    }

    private TxCardTriggerIncident createTxCardTriggerIncident(TxCard txCard, String incidentValue, String incidentName){
        TxCardTriggerIncident txCardTrigIncid = new TxCardTriggerIncident();
        txCardTrigIncid.setCardSn(txCard.getCardSn());
        txCardTrigIncid.setItemId(txCard.getItemId());
        txCardTrigIncid.setItemName(txCard.getItemName());
        txCardTrigIncid.setCrewId(txCard.getCrewId());
        txCardTrigIncid.setCrewName(txCard.getCrewName());
        txCardTrigIncid.setCloudTime(txCard.getCloudRecvTime());
        txCardTrigIncid.setReportTime(txCard.getRepTimeT8());
        txCardTrigIncid.setTriggerIncidentName(incidentName);
        txCardTrigIncid.setTriggerIncidentValue(incidentValue);
        return txCardTrigIncid;
    }

    private TxCardTriggerIncident createTxCardTriggerIncident(TxCard txCard, String incidentValue, String incidentName, String trigIncidThresholdValue, String trigIncidLastValue){
        TxCardTriggerIncident txCardTrigIncid = new TxCardTriggerIncident();
        txCardTrigIncid.setCardSn(txCard.getCardSn());
        txCardTrigIncid.setItemId(txCard.getItemId());
        txCardTrigIncid.setItemName(txCard.getItemName());
        txCardTrigIncid.setCrewId(txCard.getCrewId());
        txCardTrigIncid.setCrewName(txCard.getCrewName());
        txCardTrigIncid.setCloudTime(txCard.getCloudRecvTime());
        txCardTrigIncid.setReportTime(txCard.getRepTimeT8());
        txCardTrigIncid.setTriggerIncidentName(incidentName);
        txCardTrigIncid.setTriggerIncidentValue(incidentValue);
        txCardTrigIncid.setTrigIncidThresholdValue(trigIncidThresholdValue);
        txCardTrigIncid.setTrigIncidLastValue(trigIncidLastValue);
        return txCardTrigIncid;
    }
}
