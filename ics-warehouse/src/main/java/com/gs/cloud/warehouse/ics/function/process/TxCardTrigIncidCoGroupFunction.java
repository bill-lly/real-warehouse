package com.gs.cloud.warehouse.ics.function.process;

import com.gs.cloud.warehouse.ics.entity.ItemCrewCardDetail;
import com.gs.cloud.warehouse.ics.entity.ItemCrewCardReportTriggerIncident;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class TxCardTrigIncidCoGroupFunction implements CoGroupFunction<ItemCrewCardDetail, ItemCrewCardReportTriggerIncident, ItemCrewCardDetail> {
    @Override
    public void coGroup(Iterable<ItemCrewCardDetail> first, Iterable<ItemCrewCardReportTriggerIncident> second, Collector<ItemCrewCardDetail> out) throws Exception {
        List<ItemCrewCardDetail> itemCrewCardDetailListSrc = (List<ItemCrewCardDetail>)first;
        List<ItemCrewCardReportTriggerIncident> itemCrewCardRepTrigIncidList = (List<ItemCrewCardReportTriggerIncident>)second;
        if (itemCrewCardDetailListSrc.isEmpty()) {
            return;
        }
        //聚合
        List<ItemCrewCardDetail> itemCrewCardDetailList = aggregate(itemCrewCardDetailListSrc);
        //排序
        Collections.sort(itemCrewCardDetailList);
        Collections.sort(itemCrewCardRepTrigIncidList);
        //获取触发事件名称
        for(int itemCrewCardDetIndex = 0; itemCrewCardDetIndex < itemCrewCardDetailList.size(); itemCrewCardDetIndex++){
            ItemCrewCardDetail itemCrewCardDetail = itemCrewCardDetailList.get(itemCrewCardDetIndex);
            if(itemCrewCardDetail.getReportTime() == null){
                itemCrewCardDetail.setTriggerIncidentName(null);
                out.collect(itemCrewCardDetail);
                continue;
            }

            String trigIncidName = null;
            for (int i = 0; i < itemCrewCardRepTrigIncidList.size(); i++){
                ItemCrewCardReportTriggerIncident ele = itemCrewCardRepTrigIncidList.get(i);
                if(ele.getReportTime() == null) continue;
                if(ele.getReportTime().getTime() == itemCrewCardDetail.getReportTime().getTime()){
                    trigIncidName = ele.getTriggerIncidentName();
                    break;
                }
            }
            itemCrewCardDetail.setTriggerIncidentName(trigIncidName);
            out.collect(itemCrewCardDetail);
        }
    }

    public static List<ItemCrewCardDetail> aggregate(List<ItemCrewCardDetail> itemList) {
        // 使用Map来分组，key为cardSn和reportTime的组合
        Map<String, ItemCrewCardDetail> resultMap = new LinkedHashMap<>();

        for (ItemCrewCardDetail item : itemList) {
            String key = item.getCardSn() + "|" + item.getReportTime().getTime();

            if (resultMap.containsKey(key)) {
                // 合并positionIdList和positionNameList
                ItemCrewCardDetail existingItem = resultMap.get(key);
                existingItem.setPositionIdList(
                        DistinctStringLists(mergeStringLists(existingItem.getPositionIdList(), item.getPositionIdList()))
                );
                existingItem.setPositionNameList(
                        DistinctStringLists(mergeStringLists(existingItem.getPositionNameList(), item.getPositionNameList()))
                );
            } else {
                // 创建新条目并复制所有字段
                ItemCrewCardDetail newItem = new ItemCrewCardDetail();
                newItem.setItemId(item.getItemId());
                newItem.setItemName(item.getItemName());
                newItem.setCrewId(item.getCrewId());
                newItem.setCrewName(item.getCrewName());
                newItem.setCardSn(item.getCardSn());
                newItem.setPositionIdList(item.getPositionIdList());
                newItem.setPositionNameList(item.getPositionNameList());
                newItem.setSpaceCode(item.getSpaceCode());
                newItem.setSpaceName(item.getSpaceName());
                newItem.setBeaconCode(item.getBeaconCode());
                newItem.setTriggerIncidentName(item.getTriggerIncidentName());
                newItem.setElectricity(item.getElectricity());
                newItem.setSignalStrength(item.getSignalStrength());
                newItem.setCloudTime(item.getCloudTime());
                newItem.setReportTime(item.getReportTime());

                resultMap.put(key, newItem);
            }
        }

        return new ArrayList<>(resultMap.values());
    }

    private static String mergeStringLists(String list1, String list2) {
        if (list1 == null || list1.isEmpty()) return list2;
        if (list2 == null || list2.isEmpty()) return list1;
        return list1 + "," + list2;
    }

    private static String DistinctStringLists(String list) {
        if (list == null || list.isEmpty()) return list;
        List<String> strlist = Arrays.asList(list.split(","));
        Set<String> strSet = new LinkedHashSet<>(strlist);
        return String.join(",", strSet);
    }
}
