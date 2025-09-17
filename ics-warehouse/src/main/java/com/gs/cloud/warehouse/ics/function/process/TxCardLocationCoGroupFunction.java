package com.gs.cloud.warehouse.ics.function.process;

import com.gs.cloud.warehouse.ics.entity.ItemCrewCardDetail;
import com.gs.cloud.warehouse.ics.entity.PersonLocationBeacon;
import com.gs.cloud.warehouse.ics.entity.TxCard;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.stream.Collectors;

public class TxCardLocationCoGroupFunction implements CoGroupFunction<TxCard, PersonLocationBeacon, ItemCrewCardDetail> {
    @Override
    public void coGroup(Iterable<TxCard> first, Iterable<PersonLocationBeacon> second, Collector<ItemCrewCardDetail> out) throws Exception {
        List<TxCard> txCardList = (List<TxCard>)first;
        List<PersonLocationBeacon> personLocationBeaconList = (List<PersonLocationBeacon>)second;
        if (txCardList.isEmpty()) {
            return;
        }

        Collections.sort(txCardList);
        Collections.sort(personLocationBeaconList);
        for(int txCardListIndex = 0; txCardListIndex < txCardList.size(); txCardListIndex++){
            TxCard txCard = txCardList.get(txCardListIndex);
            //获取工牌信息生成ItemCrewCardDetail对象
            ItemCrewCardDetail itemCrewCardDetail = new ItemCrewCardDetail();
            itemCrewCardDetail.setItemId(txCard.getItemId());
            itemCrewCardDetail.setItemName(txCard.getItemName());
            itemCrewCardDetail.setCrewId(txCard.getCrewId());
            itemCrewCardDetail.setCrewName(txCard.getCrewName());
            itemCrewCardDetail.setCardSn(txCard.getCardSn());
            itemCrewCardDetail.setBeaconCode(txCard.getBeaconCode());
            itemCrewCardDetail.setElectricity(txCard.getElectricity());
            itemCrewCardDetail.setSignalStrength(txCard.getSignalStrength());
            itemCrewCardDetail.setReportTime(txCard.getRepTimeT8());
            itemCrewCardDetail.setCloudTime(txCard.getCloudRecvTime());
            if(txCard.getRepTimestampMs() == null || txCard.getRepTimestampMs().equals("")){
                itemCrewCardDetail.setSpaceCode(null);
                itemCrewCardDetail.setSpaceName(null);
                out.collect(itemCrewCardDetail);
                continue;
            }
            //获取空间值
            String spaceCode = null;
            String spaceName = null;
            for(int i = 0; i < personLocationBeaconList.size(); i++){
                PersonLocationBeacon ele = personLocationBeaconList.get(i);
                if(ele.getReportTimestamp() == null
                        || ele.getReportTimestamp().equals(""))
                    continue;
                if(ele.getReportTimestamp().equals(txCard.getRepTimestampMs())){
                    spaceCode = ele.getSpaceCode();
                    spaceName = ele.getSpaceName();
                    break;
                }
            }
            itemCrewCardDetail.setSpaceCode(spaceCode);
            itemCrewCardDetail.setSpaceName(spaceName);
            out.collect(itemCrewCardDetail);
        };
    }
}
