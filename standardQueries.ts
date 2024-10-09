// where all SQL Queries are stored

export const queryAccount = function (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string)  {
    return `SELECT
                        round(SUM(c.Conversions), 2) AS Conversions,
                        round((SUM(c.Cost) / 1000000), 2) AS Cost,
                        (CASE WHEN sum(c.Conversions) > 1 THEN round(((SUM(c.Cost)) / SUM(c.Conversions)),2) ELSE 0 END)AS CPA
                    FROM
                        \`${projectId}.${accountIdNumber}.p_CampaignBasicStats_${accountIdNumber}\` c
                        WHERE c._PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}'
                    ORDER BY
                        Conversions DESC`;
} 

export const queryAccountDevices = function (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string) {
    return `SELECT
                        c.Device,
                        round(SUM(c.Conversions), 2) AS Conversions,
                        round((SUM(c.Cost) / 1000000), 2) AS Cost,
                        (CASE WHEN sum(c.Conversions) > 1 THEN round(((SUM(c.Cost)) / SUM(c.Conversions)),2) ELSE 0 END)AS CPA
                    FROM
                        \`${projectId}.${accountIdNumber}.p_CampaignBasicStats_${accountIdNumber}\` c
                        WHERE c.Device IN UNNEST(["TABLET","DESKTOP","HIGH_END_MOBILE"])
                        AND c._PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}'
                    GROUP BY 
                        1
                    ORDER BY
                        Conversions DESC`; 
}
                                        
export const queryCampaign = function (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string, campaignIdArray:any) {
    return `SELECT 
                                c.CampaignName AS campaignName, c.CampaignId AS campaignId,
                                IFNULL(ANY_VALUE(c.CampaignDesktopBidModifier),0) AS CampaignDesktopBidModifier,
                                IFNULL(ANY_VALUE(c.CampaignMobileBidModifier),0) AS CampaignMobileBidModifier,
                                IFNULL(ANY_VALUE(c.CampaignTabletBidModifier),0) AS CampaignTabletBidModifier,
                                round(SUM(cs.Conversions), 2) AS Conversions,SUM(cs.Impressions) AS Impressions, 
                                round((SUM(cs.Cost) / 1000000),2) AS Cost
                            FROM
                                    \`${projectId}.${accountIdNumber}.p_Campaign_${accountIdNumber}\` c
                            LEFT JOIN
                                    \`${projectId}.${accountIdNumber}.p_CampaignBasicStats_${accountIdNumber}\` cs
                            ON
                                (c.CampaignId = cs.CampaignId
                                AND c._PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}' )
                                WHERE c.CampaignStatus = 'ENABLED' 
                                AND c._PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}' 
                                AND cs._PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}' 
                                AND CAST(cs.CampaignId AS STRING) IN UNNEST(${JSON.stringify(campaignIdArray)})
                                AND c.AdvertisingChannelType IN UNNEST(["SEARCH","DISPLAY"]) 
                                GROUP BY 
                                1, 2
                                ORDER BY
                                Impressions DESC `;
}

export const queryCampaignList = function (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string) {
    return `SELECT 
                                c.CampaignName, CAST(c.CampaignId AS STRING) As CampaignId, CampaignStatus
                            FROM
                                \`${projectId}.${accountIdNumber}.p_Campaign_${accountIdNumber}\` c
                            WHERE c.CampaignStatus = 'ENABLED' AND _PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}' AND ExternalCustomerId = ${accountIdNumber}
                            GROUP BY 
                                 1, 2, 3
                            ORDER BY
                                 c.CampaignName DESC`;
}
export const queryCampaignListRecent = function (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string) {
  return `SELECT 
                              c.CampaignName, CAST(c.CampaignId AS STRING) As CampaignId, cs.CampaignStatus
                          FROM
                              \`${projectId}.${accountIdNumber}.p_Campaign_${accountIdNumber}\` c
                              LEFT JOIN
                              \`${projectId}.${accountIdNumber}.p_Campaign_${accountIdNumber}\` cs
                            ON
                                (c.CampaignId = cs.CampaignId) 
                          WHERE cs._PARTITIONDATE = '${daysEnd}' AND c.CampaignStatus = 'ENABLED' AND c._PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}' AND cs.ServingStatus != 'ENDED' AND c.ExternalCustomerId = ${accountIdNumber}
                          GROUP BY 
                               1, 2, 3
                          ORDER BY
                               c.CampaignName DESC`;
}

export const queryCampaignDevices = function (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string, campaignIdArray:any) {
    return `
    WITH stats AS (
    SELECT  c.CampaignName AS campaignName, c.CampaignId AS campaignId, cs.Device AS deviceSegment,
        round(SUM(cs.Conversions), 2) AS segmentConversions,SUM(cs.Impressions) AS segmentImpressions, round((SUM(cs.Cost) / 1000000),2) AS segmentCost, round((SUM(cs.Clicks)),2) AS segmentClicks
    FROM
    \`${projectId}.${accountIdNumber}.p_Campaign_${accountIdNumber}\` c
    LEFT JOIN
    \`${projectId}.${accountIdNumber}.p_CampaignBasicStats_${accountIdNumber}\` cs
    ON
        (c.CampaignId = cs.CampaignId 
        AND c._PARTITIONDATE BETWEEN '${daysEnd}' AND '${daysEnd}')
    WHERE cs.Device IN UNNEST(["TABLET","DESKTOP","HIGH_END_MOBILE"]) AND c.AdvertisingChannelType IN UNNEST(["SEARCH","DISPLAY"]) 
    AND c.CampaignStatus = 'ENABLED' 
    AND c._PARTITIONDATE BETWEEN '${daysEnd}' AND '${daysEnd}'
    AND cs._PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}'
    GROUP BY 
        1, 2, 3
    ORDER BY
    segmentImpressions DESC
    
    )
    
    SELECT
            basic.campaignName, basic.campaignId,basic.CampaignDesktopBidModifier,basic.CampaignMobileBidModifier,basic.CampaignTabletBidModifier,
                    basic.Conversions,basic.Impressions,basic.Clicks,
                    CASE WHEN (basic.Conversions) > 0 THEN round((basic.Cost) / (basic.Conversions),2) ELSE 0 END AS cpa,
                    basic.Cost,
    ARRAY(Select AS STRUCT 
    campaignId, campaignName,
    (CASE
    WHEN deviceSegment = "HIGH_END_MOBILE" THEN "mobile" 
    WHEN deviceSegment = "TABLET" THEN "tablet" 
    WHEN deviceSegment = "DESKTOP" THEN "desktop" 
    ELSE "UNKNOWN" END) AS deviceSegment,
    CASE WHEN sum(segmentConversions) > 0 THEN round(sum(segmentCost) / sum(segmentConversions),2) ELSE 0 END AS cpa,
    CASE WHEN sum(segmentConversions) > 0 AND sum(segmentClicks) > 1 THEN round(((100 / sum(segmentClicks)) * sum(segmentConversions)),2) ELSE 0 END AS cvr,
    CASE WHEN sum(segmentClicks) > 0 THEN round((sum(segmentCost) / sum(segmentClicks)),2) ELSE 0 END AS cpc,
    round(sum(segmentCost),2) AS segmentCost, sum(segmentClicks) AS segmentClicks, round(sum(segmentConversions),2) As segmentConversions
    FROM stats WHERE basic.campaignId = campaignId  AND CAST(basic.campaignId AS STRING) IN UNNEST(${JSON.stringify(campaignIdArray)})  GROUP BY 1,2,3)  AS devices
    
    FROM (
    
    SELECT 
                    c.CampaignName AS campaignName, c.CampaignId AS campaignId,
                    IFNULL(ANY_VALUE(c.CampaignDesktopBidModifier),0) AS CampaignDesktopBidModifier,
                    IFNULL(ANY_VALUE(c.CampaignMobileBidModifier),0) AS CampaignMobileBidModifier,
                    IFNULL(ANY_VALUE(c.CampaignTabletBidModifier),0) AS CampaignTabletBidModifier,
                    round(SUM(cs.Conversions), 2) AS Conversions,SUM(cs.Impressions) AS Impressions, 
                    round(SUM(cs.Clicks), 2) AS Clicks, 
                    round((SUM(cs.Cost) / 1000000),2) AS Cost
                FROM
                \`${projectId}.${accountIdNumber}.p_Campaign_${accountIdNumber}\` c
                LEFT JOIN
                \`${projectId}.${accountIdNumber}.p_CampaignBasicStats_${accountIdNumber}\` cs
                ON
                  (c.CampaignId = cs.CampaignId
                
                    AND c._PARTITIONDATE BETWEEN '${daysEnd}' AND '${daysEnd}' )
                    WHERE c.CampaignStatus = 'ENABLED' 
                    AND c._PARTITIONDATE BETWEEN '${daysEnd}' AND '${daysEnd}'
                    AND cs._PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}'
                    
                    AND c.AdvertisingChannelType IN UNNEST(["SEARCH","DISPLAY"]) AND CAST(c.CampaignId AS STRING) IN UNNEST(${JSON.stringify(campaignIdArray)})
                    GROUP BY 
                    1, 2
                    ORDER BY
                    Impressions DESC ) as basic`;
}

export const queryPacingAccount = function (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string) {  
    return `SELECT
                                        c.Date AS dateStamp,
                                        CAST(c.Date AS STRING)AS date,
                                        Cast(round((SUM(c.Cost) / 1000000))AS INT64) AS cost
                                    FROM
                                    \`${projectId}.${accountIdNumber}.p_AccountBasicStats_${accountIdNumber}\` c
                                    WHERE _PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}'
                                    GROUP BY 
                                    1
                                    ORDER BY
                                    c.Date ASC`;
}
export const querydayOfWeekPredict = function (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string) {  
  return ` SELECT
                  dayOfWeek,
                  round(( cost / AVG(cost) OVER () ),2) AS avg
                  FROM
                  (
                SELECT 
                EXTRACT(DAYOFWEEK FROM time_series_timestamp)
                      AS dayOfWeek,
                      AVG(Cast(time_series_data AS INT64)) AS cost, 
                  FROM

                (SELECT
                *
                FROM
                ML.EXPLAIN_FORECAST(MODEL \`${projectId}.${accountIdNumber}.timeseries_account\`,
                STRUCT(30 AS horizon, 0.8 AS confidence_level))) WHERE time_series_data > 0 GROUP by 1 ) ORDER BY 
                dayOfWeek`;                                                 
}
export const querydayOfMonthPredict = function (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string) {  
  return ` SELECT
                  dayOfMonth,
                  round(( cost / AVG(cost) OVER () ),2) AS avg
                  FROM
                  (
                SELECT 
                EXTRACT(DAY FROM time_series_timestamp)
                      AS dayOfMonth,
                      AVG(Cast(time_series_data AS INT64)) AS cost, 
                  FROM

                (SELECT
                *
                FROM
                ML.EXPLAIN_FORECAST(MODEL \`${projectId}.${accountIdNumber}.timeseries_account\`,
                STRUCT(30 AS horizon, 0.8 AS confidence_level))) WHERE time_series_data > 0 GROUP by 1 )ORDER BY 
                dayOfMonth`;                                                 
}


export const querydayOfWeek = function (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string) {  
  return `SELECT
                                    dayOfWeek,
                                    ( CASE  WHEN cost > 0 THEN round(( cost / AVG(cost) OVER () ),2) ELSE 0 END) AS avg
                                  FROM(
                                  SELECT
                                         EXTRACT(DAYOFWEEK FROM c.Date)
                                         AS dayOfWeek,
                                         Cast(round((AVG(c.Cost) / 1000000))AS INT64) AS cost, 
                                     FROM
                                  \`${projectId}.${accountIdNumber}.p_AccountBasicStats_${accountIdNumber}\` c
                                  WHERE _PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}'
                                  GROUP BY 
                                  1
                                 ) ORDER BY 
                                  dayOfWeek`;                                                 
}

export const querydayOfMonth = function (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string) {  
  return `SELECT
                                    dayOfMonth,
                                    ( CASE WHEN cost > 0 THEN round(( cost / AVG(cost) OVER () ),2) ELSE 0 END) AS avg
                                  FROM(
                                  SELECT
                                         EXTRACT(DAY FROM c.Date)
                                         AS dayOfMonth,
                                         Cast(round((AVG(c.Cost) / 1000000))AS INT64) AS cost, 
                                     FROM
                                  \`${projectId}.${accountIdNumber}.p_AccountBasicStats_${accountIdNumber}\` c
                                  WHERE _PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}'
                                  GROUP BY 
                                  1
                                 ) ORDER BY 
                                 dayOfMonth`;                                                 
}
export const pacingBudgets = function (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string, campaignIdArray: any) {  
  return `WITH data AS  (
    SELECT
      (
        CASE
          WHEN RecommendedBudgetAmount IS NOT NULL THEN ROUND(CAST(RecommendedBudgetAmount AS INT64) / 1000000)
        ELSE
        NULL
      END
        ) AS recommended,
      CAST(Date(c._PARTITIONDATE) AS STRING) AS date,
      c.CampaignId,
      c.IsBudgetExplicitlyShared AS sharedBudget,
      round( Amount / 1000000,2) AS dailyBudget,
      sum(cd.Conversions) AS conversions,
      round(sum(cd.Cost / 1000000),2) AS cost,
      round(AVG(CAST (SearchBudgetLostTopImpressionShare AS FLOAT64)),2) AS budgetLostIS,
      round(MAX(CAST ( SearchBudgetLostAbsoluteTopImpressionShare AS FLOAT64)),2) AS budgetLostISAbs,
    FROM
    \`${projectId}.${accountIdNumber}.p_Campaign_${accountIdNumber}\` c
    LEFT JOIN
    \`${projectId}.${accountIdNumber}.p_CampaignCookieStats_${accountIdNumber}\` cs
        ON
            (c.CampaignId = cs.CampaignId)
    LEFT JOIN
    \`${projectId}.${accountIdNumber}.p_CampaignBasicStats_${accountIdNumber}\` cd
              ON
                  (c.CampaignId = cd.CampaignId AND c._PARTITIONTIME = cd._PARTITIONTIME)                
    WHERE
      DATE(cs._PARTITIONTIME) = '${daysEnd}' AND DATE(c._PARTITIONTIME) BETWEEN '${daysStart}' AND '${daysEnd}' AND DATE(cd._PARTITIONTIME) BETWEEN '${daysStart}' AND '${daysEnd}' AND CAST(c.CampaignId AS STRING) IN UNNEST(${JSON.stringify(campaignIdArray)}) GROUP BY 1,2,3,4,5 ORDER BY date ASC)
      
      SELECT
      dl.recommended, dl.sharedBudget, dl.CampaignId  AS campaignId,	dl.dailyBudget,	dl.budgetLostIS,	dl.budgetLostISAbs	, (dl.recommended - dl.dailyBudget )AS budgetIncrease, ARRAY_AGG( d ) AS budgets, CASE WHEN sum(d.conversions) > 0 THEN round(sum(d.cost) / sum(d.conversions),2) ELSE 0 END AS cpa, round(sum(d.cost),2) AS cost, round(sum(d.conversions),2) AS conversions
     FROM data d
    LEFT JOIN
    data dl  ON
            (d.CampaignId = dl.CampaignId) WHERE dl.date = '${daysEnd}'
    GROUP BY 1,2,3,4,5,6`;                                                 
}


export const queryPacingGroup = function (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string, campaignIdArray:any) {
    return `SELECT
                                    c.Date AS dateStamp,
                                    CAST(c.Date AS STRING)AS date,
                                    Cast(round((SUM(c.Cost) / 1000000))AS INT64) AS cost
                                FROM
                                \`${projectId}.${accountIdNumber}.p_CampaignBasicStats_${accountIdNumber}\` c
                                WHERE _PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}' AND CAST(c.CampaignId AS STRING) IN UNNEST(${JSON.stringify(campaignIdArray)})
                                GROUP BY 
                                 1
                                 ORDER BY
                                 c.Date ASC`;
}
export const configShopping = function (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string, campaignIdArray:any) {
  return `SELECT
        ROUND(SUM(basic.cost) / SUM(basic.conversions),2) AS targetCPA,
        ROUND(SUM(basic.cost)) AS cost,
         ROUND(SUM(basic.conversionValue)) AS conversionValue,
        ROUND(AVG(basic.conversions)) AS avgConversions,
        ROUND(MAX(basic.maxCurrentBid ),2) AS maxCPC,
        ROUND(MIN(basic.maxCurrentBid ),2) AS minCPC,
        ROUND(AVG(basic.conversions) / 4)  AS minConversions,
        ROUND(AVG(basic.searchIS + 10)) AS ISTarget,
        CASE WHEN sum(basic.conversionValue) > 0 THEN CAST(round( sum(basic.conversionValue) / sum(basic.cost),2)AS FLOAT64) ELSE 0 END AS ROAS
        FROM (
        SELECT
            c.AdGroupId AS adGroupId,
            Max(CAST(REGEXP_REPLACE(c.CpcBid, '[^0-9]+', '') AS INT64)/ 1000000) AS maxCurrentBid,
            ROUND((SUM(cs.Cost) / 1000000),2) AS cost,
            SUM(cs.Clicks) AS clicks,
            ROUND((SUM(cs.Conversions)),2) AS conversions,
            CAST( SUM(cs.ConversionValue) AS INT64) AS conversionValue,
            round((AVG(IFNULL(CAST(regexp_replace((cs.SearchImpressionShare ), '[^a-zA-Z0-9]', '')AS float64)/100, 0))),2)   AS searchIS, 
           
        FROM
        \`${projectId}.${accountIdNumber}.p_Criteria_${accountIdNumber}\` c  
      LEFT JOIN
      \`${projectId}.${accountIdNumber}.p_ShoppingProductStats_${accountIdNumber}\` cs
      ON
       ( c.CampaignId = cs.CampaignId AND c.AdGroupId = cs.AdGroupId AND c._PARTITIONDATE = cs._PARTITIONDATE AND	cs.OfferId = REPLACE(c.Criteria, 'id==', '') )   WHERE c._PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}' AND CriteriaType = "PRODUCT_PARTITION" AND CAST(c.CampaignId AS STRING) IN UNNEST(${JSON.stringify(campaignIdArray)})
       
        GROUP BY
            1 ) AS basic`;
}
export const configDSA = function (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string, campaignIdArray:any) {
    return `SELECT
        ROUND(SUM(basic.cost) / SUM(basic.conversions),2) AS targetCPA,
        ROUND(AVG(basic.conversions)) AS avgConversions,
        ROUND(MAX(basic.maxCurrentBid ),2) AS maxCPC,
        ROUND(MIN(basic.maxCurrentBid ),2) AS minCPC,
        ROUND(AVG(basic.conversions) / 4)  AS minConversions,
        ROUND(AVG(enhanced.searchIS + 10)) AS ISTarget
        FROM (
        SELECT
            c.AdGroupId AS adGroupId,
            CAST(c.CpcBid AS INT64)/ 1000000 AS maxCurrentBid,
            ROUND((SUM(cs.Cost) / 1000000),2) AS cost,
            SUM(cs.Clicks) AS clicks,
            ROUND((SUM(cs.Conversions)),2) AS conversions,
            CAST( SUM(cs.ConversionValue) AS INT64) AS conversionValue
        FROM
        \`${projectId}.${accountIdNumber}.p_AdGroup_${accountIdNumber}\` c
        LEFT JOIN
        \`${projectId}.${accountIdNumber}.p_AdGroupBasicStats_${accountIdNumber}\` cs
        ON
            (c.CampaignId = cs.CampaignId
            AND c.AdGroupId = cs.AdGroupId
            AND c._PARTITIONDATE BETWEEN '${daysEnd}' AND '${daysEnd}'
            AND cs._PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}' )
        WHERE
            c.AdGroupStatus = 'ENABLED'
            AND c._PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}'
            AND c.AdGroupType = "SEARCH_DYNAMIC_ADS"
            AND NOT REGEXP_CONTAINS( c.CpcBid, "auto")
            AND CAST(c.CampaignId AS STRING) IN UNNEST(${JSON.stringify(campaignIdArray)})
        GROUP BY
            1,2
        ORDER BY
            cost DESC ) AS basic
        LEFT JOIN (
        SELECT
            ct.AdGroupId AS adGroupId,
            ROUND((AVG(IFNULL(CAST(REGEXP_REPLACE((ct.SearchImpressionShare), '[^a-zA-Z0-9]', '')AS float64)/100,
                    0))),2) AS searchIS,
            ROUND((AVG(IFNULL(CAST(REGEXP_REPLACE((ct.SearchBudgetLostAbsoluteTopImpressionShare ), '[^a-zA-Z0-9]', '')AS float64)/100,
                    0))),2) AS searchIShareBudgetLost,
        FROM
           \`${projectId}.${accountIdNumber}.p_AdGroupCrossDeviceStats_${accountIdNumber}\` ct
        WHERE CAST(ct.CampaignId AS STRING) IN UNNEST(${JSON.stringify(campaignIdArray)})
        GROUP BY
            1 ) AS enhanced
        ON
        basic.adGroupId = enhanced.adGroupId
        WHERE basic.conversions > 1 `;
} 
export const queryShoppingBidding = function (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string,campaignIdArray:any) {
  return `WITH stats AS ( 

    SELECT     
      c.AdGroupId,
      c.CampaignId,
      CAST((c._PARTITIONDATE)AS STRING) AS date,
      (CASE WHEN STARTS_WITH(c.CpcBid, 'auto: ') THEN "automatic" ELSE "manual" END ) AS bidType,
      Brand,
      ProductTypeL1,
      cs.OfferId,
      max(CAST(REPLACE(c.CpcBid, 'auto ', '') AS INT64)/ 1000000) AS bid,
      round((SUM(cs.Cost) / 1000000),2) AS cost,
      SUM(cs.Clicks) AS clicks,
      round((SUM(cs.Conversions)),2) AS conversions,
      CAST(SUM(cs.ConversionValue )AS INT64) AS conversionValue,
      CASE WHEN sum(cs.conversions) > 0 THEN round(sum(cs.cost / 1000000) / sum(cs.conversions),2) ELSE 0 END AS CPA,  
      CASE WHEN sum(cs.conversions) > 0 AND sum(cs.clicks) > 1 THEN round(((100 / sum(cs.clicks)) * sum(cs.conversions)),2) ELSE 0 END AS CVR,
      CASE WHEN sum(cs.clicks) > 0 THEN round(sum(cs.clicks) / sum(cs.impressions),4) ELSE 0 END AS CTR,
       round((AVG(IFNULL(CAST(regexp_replace((cs.SearchAbsoluteTopImpressionShare), '[^a-zA-Z0-9]', '')AS float64)/100, 0))),2)  AS searchTopIS,
            round((AVG(IFNULL(CAST(regexp_replace((cs.SearchClickShare), '[^a-zA-Z0-9]', '')AS float64)/100, 0))),2) AS searchClickShare,
            round((AVG(IFNULL(CAST(regexp_replace((cs.SearchImpressionShare ), '[^a-zA-Z0-9]', '')AS float64)/100, 0))),2)   AS searchIS, 
          FROM
          \`${projectId}.${accountIdNumber}.p_Criteria_${accountIdNumber}\` c  
          LEFT JOIN
          \`${projectId}.${accountIdNumber}.p_ShoppingProductStats_${accountIdNumber}\` cs
          ON
           ( c.CampaignId = cs.CampaignId AND c.AdGroupId = cs.AdGroupId AND c._PARTITIONDATE = cs._PARTITIONDATE AND	cs.OfferId = REPLACE(c.Criteria, 'id==', '') )   WHERE c._PARTITIONDATE BETWEEN  '${daysStart}' AND '${daysEnd}' AND CriteriaType = "PRODUCT_PARTITION" AND cs.cost IS NOT NULL GROUP By 1,2,3,4,5,6,7) 
           
           SELECT  basic.CampaignId AS campaignId, basic.adGroupName, basic.AdGroupId AS adGroupId, CAST(basic.currentBid AS INT64)/ 1000000 AS currentBid, basic.bidType, brand,
      basic.productType1,
      basic.Id, round(basic.cost,2) AS cost, basic.clicks, basic.conversions,  basic.conversionValue, searchIS,  CASE WHEN sum(conversions) > 0 THEN round(sum(cost) / sum(conversions),2) ELSE 0 END AS CPA, CASE WHEN sum(conversionValue) > 0 THEN CAST(round( sum(conversionValue) / sum(cost),2)AS FLOAT64)   ELSE 0 END AS ROAS,  CASE WHEN sum(conversions) > 0 AND sum(clicks) > 1 THEN round(((100 / sum(clicks)) * sum(conversions)),2) ELSE 0 END AS CVR,  CASE WHEN sum(clicks) > 0 THEN round(sum(cost) / sum(clicks),2) ELSE 0 END AS CPC, ARRAY(Select AS STRUCT date, bid, CASE WHEN sum(conversions) > 0 THEN round(sum(cost) / sum(conversions),2) ELSE 0 END AS CPA,   CASE WHEN sum(conversions) > 0 AND sum(clicks) > 1 THEN round(((100 / sum(clicks)) * sum(conversions)),2) ELSE 0 END AS CVR, CASE WHEN sum(clicks) > 0 THEN round(sum(cost) / sum(clicks),2) ELSE 0 END AS CPC,  round(sum(cost),2) AS cost , sum(clicks) AS clicks, sum(conversions) As conversions, sum(conversionValue) AS conversionValue,  FROM stats WHERE basic.AdGroupId = AdGroupId AND basic.CampaignId = CampaignId GROUP BY 1,2) AS stats, 
    
    
    ARRAY(Select AS STRUCT bid, min(stats.date) AS date, round(avg(searchIS)) AS searchIS, CASE WHEN sum(conversions) > 0 THEN round(sum(cost) / sum(conversions),2) ELSE 0 END AS CPA,  CASE WHEN sum(conversions) > 0 AND sum(clicks) > 1 THEN round(((100 / sum(clicks)) * sum(conversions)),2) ELSE 0 END AS CVR,  CASE WHEN sum(clicks) > 0 THEN round(sum(cost) / sum(clicks),2) ELSE 0 END AS CPC, round(sum(cost),2) AS cost, sum(clicks) AS clicks, sum(conversions) As conversions, sum(conversionValue) AS conversionValue   FROM stats WHERE  basic.AdGroupId = AdGroupId AND basic.CampaignId = CampaignId GROUP BY 1) AS bid
    FROM
    ( 
    SELECT                            
          AdGroupName AS adGroupName,
          c.AdGroupId,
          c.CampaignId,
          Brand AS brand,
          ProductTypeL1 AS productType1,
          cs.OfferId AS Id,
          REGEXP_REPLACE(c.CpcBid, r'\$|,', '') AS currentBid,
          (CASE WHEN STARTS_WITH(c.CpcBid, 'auto ') THEN "automatic" ELSE "manual" END ) AS bidType,
          round(SUM(cs.cost),2) AS cost,
          SUM(cs.clicks) AS clicks,
          round((SUM(cs.conversions)),2) AS conversions,
          CAST(SUM(cs.conversionValue )AS INT64) AS conversionValue, 
          round(avg(searchIS)) AS searchIS,
              FROM
              \`${projectId}.${accountIdNumber}.p_AdGroup_${accountIdNumber}\` c                            
              LEFT JOIN stats cs                                
                  ON
                  ( cs.AdGroupId = c.AdGroupId AND cs.campaignId = c.campaignId )
                  WHERE c._PARTITIONDATE = '${daysEnd}' AND CAST(c.CampaignId AS STRING) IN UNNEST(${JSON.stringify(campaignIdArray)})
    GROUP BY 
          1,2,3,4,5,6,7,8
         ORDER BY
          cost DESC
    ) AS basic WHERE cost is not null AND cost > 0 AND conversions	> 0
    GROUP BY 
    1,2,3,4,5,6,7,8,9,10,11,12,13 
     `;
}

export const queryDSABidding = function (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string, campaignIdArray:any) {
  return `SELECT basic.campaignId, basic.adGroupId, basic.adGroupName, basic.currentBid, basic.cost, basic.clicks, basic.conversions,  basic.conversionValue, enhanced.searchIS, enhanced.searchISLost, enhanced.searchIShareBudgetLost, round((SUM((100 / clicks) * conversions)),2) AS CVR 
    FROM
    (SELECT 
                              AdGroupName AS adGroupName,
                              CAST(c.AdGroupId AS STRING) AS adGroupId,
                              CAST(c.CampaignId  AS STRING) AS campaignId,
                              CAST(c.CpcBid AS INT64)/ 1000000 AS currentBid,
                              round((SUM(cs.Cost) / 1000000),2) AS cost,
                              SUM(cs.Clicks) AS clicks,
                              round((SUM(cs.Conversions)),2) AS conversions,
                              CAST(SUM(cs.ConversionValue )AS INT64) AS conversionValue
                                  FROM
                                  \`${projectId}.${accountIdNumber}.p_AdGroup_${accountIdNumber}\` c
                                  LEFT JOIN
                                  \`${projectId}.${accountIdNumber}.p_AdGroupBasicStats_${accountIdNumber}\` cs
                                      ON
                              (c.CampaignId = cs.CampaignId AND c.AdGroupId = cs.AdGroupId
                              AND c._PARTITIONDATE BETWEEN '${daysEnd}' AND '${daysEnd}' AND cs._PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}'  )
                              WHERE c.AdGroupStatus = 'ENABLED' AND c._PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}' AND c.AdGroupType = "SEARCH_DYNAMIC_ADS" AND CAST(c.CampaignId AS STRING) IN UNNEST(${JSON.stringify(campaignIdArray)})
                              GROUP BY 
                              1,2,3,4
                             ORDER BY
                              cost DESC
                              ) AS basic
                              LEFT JOIN
 (
    SELECT     
                         CAST(ct.AdGroupId AS STRING) AS adGroupId,
                         CAST(ct.CampaignId  AS STRING) AS campaignId,
                         round((AVG(IFNULL(CAST(regexp_replace((ct.SearchImpressionShare), '[^a-zA-Z0-9]', '')AS float64)/100, 0))),2)  AS searchIS,
                         round((AVG(IFNULL(CAST(regexp_replace((ct.SearchRankLostImpressionShare), '[^a-zA-Z0-9]', '')AS float64)/100, 0))),2) AS searchISLost,
                         round((AVG(IFNULL(CAST(regexp_replace((ct.SearchBudgetLostAbsoluteTopImpressionShare ), '[^a-zA-Z0-9]', '')AS float64)/100, 0))),2)   AS searchIShareBudgetLost,                  
                     FROM
                     \`${projectId}.${accountIdNumber}.p_AdGroupCrossDeviceStats_${accountIdNumber}\` ct
                     WHERE CAST(ct.CampaignId AS STRING) IN UNNEST(${JSON.stringify(campaignIdArray)})  
                     GROUP BY 
                         1,2
) AS enhanced
ON basic.adGroupId = enhanced.adGroupId AND basic.campaignId = enhanced.campaignId
WHERE basic.conversions > 1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11`;
}

export const statsQueryDSABidding = function (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string,campaignIdArray:any) {
    return `SELECT 
                                
                                CAST(c.AdGroupId AS STRING) AS adGroupId,
                                CAST(c.CampaignId  AS STRING) AS campaignId,
                                CAST((cs.Date)AS STRING) AS date,
                                round((SUM(cs.Cost) / 1000000),2) AS cost,
                                 SUM(cs.Clicks) AS clicks,
                                round((SUM(cs.Conversions)),2) AS conversions,
                                CAST(SUM(cs.ConversionValue )AS INT64) AS conversionValue
                                    FROM
                                    \`${projectId}.${accountIdNumber}.p_AdGroup_${accountIdNumber}\` c
                                    LEFT JOIN
                                    \`${projectId}.${accountIdNumber}.p_AdGroupBasicStats_${accountIdNumber}\` cs
                                        ON
                                (c.CampaignId = cs.CampaignId AND c.AdGroupId = cs.AdGroupId
                                AND c._PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}' AND cs._PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}'  )
                                WHERE c.AdGroupStatus = 'ENABLED' AND c._PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}' AND c.AdGroupType = "SEARCH_DYNAMIC_ADS" AND CAST(c.CampaignId AS STRING) IN UNNEST(${JSON.stringify(campaignIdArray)})
                                GROUP BY 
                                1,2,3
                               ORDER BY
                                date DESC`;
}
export const queryDSABiddingStatsHistorical = function (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string,campaignIdArray:any) {
    return `SELECT                                     
                                CAST(c.AdGroupId AS STRING) AS adGroupId,
                                CAST(c.CampaignId  AS STRING) AS campaignId,
                                CAST(c.CpcBid AS STRING) historicalBid,
                                CAST(c.CpcBid AS INT64)/ 1000000 AS bid,
                                CAST((c._PARTITIONDATE)AS STRING) AS date,                         
                                    FROM
                                    \`${projectId}.${accountIdNumber}.p_AdGroup_${accountIdNumber}\` c                                 
                                WHERE c.AdGroupStatus = 'ENABLED' AND c._PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}' AND c.AdGroupType = "SEARCH_DYNAMIC_ADS" AND CAST(c.CampaignId AS STRING) IN UNNEST(${JSON.stringify(campaignIdArray)})
                               ORDER BY
                                date DESC`;
}
export const configKeyword = function (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string, campaignIdArray:any )  {
    return `SELECT
  (CASE WHEN sum(basic.conversions) > 1 THEN round(((SUM(basic.cost)) / SUM(basic.conversions)),2) ELSE 0 END)AS targetCPA,
  ROUND(AVG(basic.conversions)) AS avgConversions,
  ROUND(MAX(basic.maxCurrentBid ),2) AS maxCPC,
  ROUND(MIN(basic.maxCurrentBid ),2) AS minCPC,
  ROUND(AVG(basic.conversions) / 4)  AS minConversions,
  ROUND(AVG(enhanced.searchIS + 10)) AS ISTarget,
FROM (
  SELECT
    c.AdGroupId AS adGroupId,
    c.Criteria,
    c.KeywordMatchType AS MatchType,
    CAST(c.CpcBid AS INT64)/ 1000000 AS maxCurrentBid,
    ROUND((SUM(cs.Cost) / 1000000),2) AS cost,
    SUM(cs.Clicks) AS clicks,
    ROUND((SUM(cs.Conversions)),2) AS conversions,
    CAST( SUM(cs.ConversionValue) AS INT64) AS conversionValue
    FROM
    \`${projectId}.${accountIdNumber}.p_Keyword_${accountIdNumber}\` c                                  
    LEFT JOIN  \`${projectId}.${accountIdNumber}.p_KeywordBasicStats_${accountIdNumber}\` cs 
    ON
    (cs.CriterionId = c.CriterionId AND cs.AdGroupId = c.AdGroupId AND cs.campaignId = c.campaignId
    AND c._PARTITIONDATE BETWEEN '${daysEnd}' AND '${daysEnd}' AND cs._PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}'  )
  WHERE c.Status = 'ENABLED' AND CAST(c.CampaignId AS STRING) IN UNNEST(${JSON.stringify(campaignIdArray)})  
    AND c._PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}' AND cs.conversions > 1 AND c.isNegative = false AND NOT REGEXP_CONTAINS( c.CpcBid, "auto")
  GROUP BY
    1,2,3,4
  ORDER BY
    cost DESC ) AS basic
LEFT JOIN (
  SELECT
    ct.AdGroupId AS adGroupId,
    ROUND((AVG(IFNULL(CAST(REGEXP_REPLACE((ct.SearchImpressionShare), '[^a-zA-Z0-9]', '')AS float64)/100,
            0))),2) AS searchIS,
    ROUND((AVG(IFNULL(CAST(REGEXP_REPLACE((ct.SearchBudgetLostAbsoluteTopImpressionShare ), '[^a-zA-Z0-9]', '')AS float64)/100,
            0))),2) AS searchIShareBudgetLost,
  FROM
  \`${projectId}.${accountIdNumber}.p_KeywordCrossDeviceStats_${accountIdNumber}\` ct
  WHERE CAST(ct.CampaignId AS STRING) IN UNNEST(${JSON.stringify(campaignIdArray)}) 
  GROUP BY
    1 ) AS enhanced
ON
  basic.adGroupId = enhanced.adGroupId
WHERE
  basic.conversions > 1 `;
}
export const configKeywordMatchType = function (projectId:string,accountIdNumber: number, daysStart: string, daysEnd: string, campaignIdArray:any )  {
    return `SELECT
  regexp_CONTAINS((basic.Criteria), '[+]') as Modified,
  basic.MatchType,
  ROUND(SUM(basic.cost) / SUM(basic.conversions)*(1-(AVG(enhanced.searchIShareBudgetLost)/100)),2) AS targetCPA,
FROM (
  SELECT
    c.AdGroupId AS adGroupId,
    c.Criteria,
    c.KeywordMatchType AS MatchType,
    ROUND((SUM(cs.Cost) / 1000000),2) AS cost,
    ROUND((SUM(cs.Conversions)),2) AS conversions,
    FROM
    \`${projectId}.${accountIdNumber}.p_Keyword_${accountIdNumber}\` c                                  
    LEFT JOIN  \`${projectId}.${accountIdNumber}.p_KeywordBasicStats_${accountIdNumber}\` cs 
    ON
    (cs.CriterionId = c.CriterionId AND cs.AdGroupId = c.AdGroupId AND cs.campaignId = c.campaignId
    AND c._PARTITIONDATE BETWEEN '${daysEnd}' AND '${daysEnd}' AND cs._PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}'  )
  WHERE
    c.Status = 'ENABLED' AND CAST(c.CampaignId AS STRING) IN UNNEST(${JSON.stringify(campaignIdArray)}) 
    AND c._PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}' AND cs.conversions > 1 AND c.isNegative = false
  GROUP BY
    1,2,3
  ORDER BY
    cost DESC ) AS basic
LEFT JOIN (
  SELECT
    ct.AdGroupId AS adGroupId,
    ROUND((AVG(IFNULL(CAST(REGEXP_REPLACE((ct.SearchBudgetLostAbsoluteTopImpressionShare ), '[^a-zA-Z0-9]', '')AS float64)/100,
            0))),2) AS searchIShareBudgetLost,
  FROM
  \`${projectId}.${accountIdNumber}.p_KeywordCrossDeviceStats_${accountIdNumber}\` ct
  WHERE CAST(ct.CampaignId AS STRING) IN UNNEST(${JSON.stringify(campaignIdArray)}) 
  GROUP BY
    1 ) AS enhanced
ON
  basic.adGroupId = enhanced.adGroupId
WHERE
  basic.conversions > 1 GROUP BY 
  1,2`;
}
export const queryKeywordBidding = function (projectId:string,accountIdNumber: number, daysStart: string, daysEnd: string,campaignIdAStatement:string, whereStatement: string )  {
    return `SELECT basic.campaignId, basic.adGroupId, basic.keywordId, basic.keyword, basic.matchType, basic.currentBid, basic.QS, basic.firstPageCpc,  basic.firstPositionCpc, basic.estimatedAddClicksAtFirstPositionCpc, basic.predictedCtr, basic.adRelevance, basic.landingPageExp,  basic.cost, basic.clicks, basic.conversions,  basic.conversionValue, enhanced.searchIS, enhanced.searchISLost, enhanced.searchIShareBudgetLost, round((SUM((100 / clicks) * conversions)),2) AS CVR
    FROM
      (
        SELECT                            
                              Criteria AS keyword,
                              CAST(c.CriterionId AS STRING) AS keywordId,
                              CAST(c.AdGroupId AS STRING) AS adGroupId,
                              CAST(c.CampaignId  AS STRING) AS campaignId,
                              c.KeywordMatchType AS matchType,
                              CAST(c.FirstPageCpc AS INT64)/ 1000000 AS firstPageCpc,
                              CAST(c.FirstPositionCpc AS INT64)/ 1000000 AS firstPositionCpc,
                              c.EstimatedAddClicksAtFirstPositionCpc AS estimatedAddClicksAtFirstPositionCpc,
                              c.SearchPredictedCtr AS predictedCtr, 
                              c.CreativeQualityScore AS adRelevance,                               
                              c.PostClickQualityScore AS landingPageExp,
                              CAST(c.CpcBid AS INT64)/ 1000000 AS currentBid,
                              c.QualityScore AS QS, 
                              round((SUM(cs.Cost) / 1000000),2) AS cost,
                              SUM(cs.Clicks) AS clicks,
                              round((SUM(cs.Conversions)),2) AS conversions,
                              SUM(cs.Impressions) AS impressions,
                              CAST(SUM(cs.Impressions)AS INT64) AS conversionValue,                       
                                  FROM
                                  \`${projectId}.${accountIdNumber}.p_Keyword_${accountIdNumber}\` c                                  
                                  LEFT JOIN  \`${projectId}.${accountIdNumber}.p_KeywordBasicStats_${accountIdNumber}\` cs                                
                                      ON
                                      (cs.CriterionId = c.CriterionId AND cs.AdGroupId = c.AdGroupId AND cs.campaignId = c.campaignId
                                      AND c._PARTITIONDATE BETWEEN '${daysEnd}' AND '${daysEnd}' AND cs._PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}'  )
                                      WHERE c.Status = 'ENABLED' AND c.isNegative = false AND c._PARTITIONDATE BETWEEN '${daysEnd}' AND '${daysEnd}' AND c.BiddingStrategyType = "cpc" AND c.HasQualityScore = true AND cs.clicks > 0 ${campaignIdAStatement} AND NOT REGEXP_CONTAINS( c.CpcBid, "auto")
                        GROUP BY 
                              1,2,3,4,5,6,7,8,9,10,11,12,13
                             ORDER BY
                              cost DESC
      ) AS basic
    LEFT JOIN
      (
         SELECT     
                              CAST(ct.CriterionId AS STRING) AS keywordId,
                              CAST(ct.AdGroupId AS STRING) AS adGroupId,
                              CAST(ct.CampaignId  AS STRING) AS campaignId,
                              round((AVG(IFNULL(CAST(regexp_replace((ct.SearchImpressionShare), '[^a-zA-Z0-9]', '')AS float64)/100, 0))),2)  AS searchIS,
                              round((AVG(IFNULL(CAST(regexp_replace((ct.SearchRankLostImpressionShare), '[^a-zA-Z0-9]', '')AS float64)/100, 0))),2) AS searchISLost,
                              round((AVG(IFNULL(CAST(regexp_replace((ct.SearchBudgetLostAbsoluteTopImpressionShare ), '[^a-zA-Z0-9]', '')AS float64)/100, 0))),2)   AS searchIShareBudgetLost,                  
                          FROM
                          \`${projectId}.${accountIdNumber}.p_KeywordCrossDeviceStats_${accountIdNumber}\` ct
                            GROUP BY 
                              1,2,3    
    ) AS enhanced
      ON basic.keywordId = enhanced.keywordId AND basic.adGroupId = enhanced.adGroupId AND basic.campaignId = enhanced.campaignId WHERE ${whereStatement}
      GROUP BY 
      1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20 

`;
}
export const statsQueryKeywordBidding = function (projectId:string,accountIdNumber: number, daysStart: string, daysEnd: string, campaignIdAStatement:string, keywordIds:any) {
    return `SELECT 
                                Criteria AS keyword,
                                CAST(c.CriterionId AS STRING) AS keywordId,
                                CAST(c.AdGroupId AS STRING) AS adGroupId,
                                CAST(c.CampaignId  AS STRING) AS campaignId,
                                c.KeywordMatchType AS matchType,
                                CAST((cs.Date)AS STRING) AS date,
                                round((SUM(cs.Cost) / 1000000),2) AS cost,
                                SUM(cs.Clicks) AS clicks,
                                round((SUM(cs.Conversions)),2) AS conversions,
                                CAST(SUM(cs.ConversionValue )AS INT64) AS conversionValue
                                    FROM
                                    \`${projectId}.${accountIdNumber}.p_Keyword_${accountIdNumber}\` c
                                    LEFT JOIN
                                    \`${projectId}.${accountIdNumber}.p_KeywordBasicStats_${accountIdNumber}\` cs
                                        ON
                                (c.CriterionId = cs.CriterionId AND c.AdGroupId = cs.AdGroupId
                                AND c._PARTITIONDATE BETWEEN '${daysEnd}' AND '${daysEnd}' AND cs._PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}'  )
                                WHERE c.Status = 'ENABLED' AND c._PARTITIONDATE BETWEEN '${daysEnd}' AND '${daysEnd}' AND CAST(c.CriterionId AS STRING) IN UNNEST(${JSON.stringify(keywordIds)}) ${campaignIdAStatement}
                                GROUP BY 
                                1,2,3,4,5,6
                               ORDER BY
                                cost DESC`;
}
export const queryKeywordBiddingStatsHistorical  = function (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string, campaignIdAStatement:string, keywordIds:any) {
    return `SELECT 
                               
                                CAST(c.CriterionId AS STRING) AS keywordId,
                                CAST(c.AdGroupId AS STRING) AS adGroupId,
                                CAST(c.CampaignId  AS STRING) AS campaignId,
                                CAST(c.CpcBid AS STRING) historicalBid,
                                CAST(c.CpcBid AS INT64)/ 1000000 AS bid,
                                CAST((c._PARTITIONDATE)AS STRING) AS date,   
                                    FROM
                                    \`${projectId}.${accountIdNumber}.p_Keyword_${accountIdNumber}\` c
                                WHERE c.Status = 'ENABLED' AND c._PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}' AND CAST(c.CriterionId AS STRING) IN UNNEST(${JSON.stringify(keywordIds)})  ${campaignIdAStatement} AND NOT REGEXP_CONTAINS( c.CpcBid, "auto")
                                
                               ORDER BY
                                date DESC`;
}

export const queryNegativeFunnels = (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string, campaignIdArray : any) => {
  return `With AdGroupTable AS 
  (
  SELECT
    c.query,
    c.CriterionId AS keywordId,
    cu.Criteria AS keyword,
    cu.QualityScore AS QS, 
    cu.KeywordMatchType AS matchType,
    c.queryTargetingStatus AS targetingStatus,
    c.CampaignId AS campaignId,
    ct.CampaignName AS campaignName,
    c.AdGroupId AS adGroupId,
    CAST(c.AdGroupId AS STRING) AS adGroupIdString,
    cs.AdGroupName AS adGroupName,
    round((SUM(c.Cost) / 1000000),2) AS cost,
    SUM(c.Impressions) AS impressions,
    SUM(c.Clicks) AS clicks,
    round((SUM(c.Conversions)),2) AS conversions
    FROM
    \`${projectId}.${accountIdNumber}.p_SearchQueryStats_${accountIdNumber}\`c
    LEFT JOIN
    \`${projectId}.${accountIdNumber}.p_AdGroup_${accountIdNumber}\`cs
    ON
    (c.CampaignId = cs.CampaignId AND c.AdGroupId = cs.AdGroupId)
    LEFT JOIN
    \`${projectId}.${accountIdNumber}.p_Campaign_${accountIdNumber}\`ct
    ON
    (c.CampaignId = ct.CampaignId)
    LEFT JOIN
    \`${projectId}.${accountIdNumber}.p_Keyword_${accountIdNumber}\`cu
    ON
    (c.CampaignId = cu.CampaignId AND c.AdGroupId = cu.AdGroupId AND c.CriterionId = cu.CriterionId)
    WHERE c._PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}' AND ct._PARTITIONDATE BETWEEN '${daysEnd}' AND '${daysEnd}' AND cs._PARTITIONDATE BETWEEN '${daysEnd}' AND '${daysEnd}' AND cu._PARTITIONDATE BETWEEN '${daysEnd}' AND '${daysEnd}' AND cu.Status = 'ENABLED' AND cu.isNegative = false AND QueryTargetingStatus != "EXCLUDED" AND c.query IS NOT NULL
    GROUP BY
    1,2,3,4,5,6,7,8,9,10,11 ORDER BY cost DESC
  )
  SELECT 
  out.campaignId,
  out.campaignName,
  out.query, 
  out.adgroupIdCount,
  out.exactCount,
  out.phraseCount,
  out.broadCount,
  out.bmmCount,
  out.cost,
  out.totalClicks,
  out.impressions,
  out.clicks,
  out.conversions,
  out.maxQS,
  out.targetingStatus,
  out.data,   
FROM(  
  SELECT 
    overall.campaignId,
    t.campaignName,
    overall.query, 
    overall.adgroupIdCount,
    SUM(CASE WHEN t.matchType = "EXACT" THEN 1 ELSE 0 END) AS exactCount,
    SUM(CASE WHEN t.matchType = "PHRASE" THEN 1 ELSE 0 END) AS phraseCount,
    SUM(CASE WHEN t.matchType = "BROAD" AND regexp_CONTAINS((t.keyword), '^[a-zA-Z0-9 ]*$') THEN 1 ELSE 0 END) AS broadCount,
    SUM(CASE WHEN t.matchType = "BROAD" AND regexp_CONTAINS((t.keyword), '[+]') THEN 1 ELSE 0 END) AS bmmCount,
    (SELECT SUM(clicks) FROM  \`AdGroupTable\`) AS totalClicks,
    round(SUM(t.cost),2) AS cost,
    SUM(t.impressions) AS impressions,
    SUM(t.clicks) AS clicks,
    round(SUM(t.conversions),2) AS conversions,
    MAX(t.QS) AS maxQS,
    STRING_AGG(DISTINCT TO_JSON_STRING(overall.targetingStatus)) AS targetingStatus,
    ARRAY_AGG(t) AS data,   

  FROM(  
  SELECT c.query,
  c.CampaignId AS campaignId,
  COUNT(DISTINCT c.AdgroupId ) AS AdgroupIdCount,
  ARRAY_AGG(DISTINCT CASE
  WHEN ENDS_WITH(AdGroupName , "BMM") THEN "BMM"
  WHEN ENDS_WITH(AdGroupName , "PHRASE") THEN "PHRASE"
  WHEN ENDS_WITH(AdGroupName , "PHRAS") THEN "PHRASE"
  WHEN ENDS_WITH(AdGroupName , "BROAD") THEN "BROAD"
  WHEN ENDS_WITH(AdGroupName , "EXACT") THEN "EXACT"
  ELSE "Unknown" END) AS AdGrouptype,
  ARRAY_AGG(DISTINCT c.QueryTargetingStatus) AS targetingStatus,
  FROM
  \`${projectId}.${accountIdNumber}.p_SearchQueryStats_${accountIdNumber}\`c
    LEFT JOIN
    \`${projectId}.${accountIdNumber}.p_AdGroup_${accountIdNumber}\`cs
    ON
    (c.CampaignId = cs.CampaignId AND c.AdGroupId = cs.AdGroupId)
    WHERE c._PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}' AND cs._PARTITIONDATE BETWEEN '${daysEnd}' AND '${daysEnd}' AND AdGroupStatus = "ENABLED"
    GROUP BY
    1,2) AS overall
     LEFT JOIN 
     \`AdGroupTable\` t 
     ON
    (overall.Query = t.Query AND overall.CampaignId = t.CampaignId)
     WHERE overall.AdgroupIdCount > 1 GROUP BY
    1,2,3,4) AS out WHERE out.conversions > 1`;
}

export const querySearchTermInsights = (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string, statement:string, t:any ) => {
  return `   WITH data AS  (
    SELECT 
     _PARTITIONDATE AS date,
     Query AS QueryRaw,
     CampaignId,
     ARRAY_AGG(CAST(CampaignId AS STRING)) AS CampaignId,
     SUM(Conversions) AS tConversions,
     (SUM(Cost) / 1000000) AS tCost,
     SUM(Impressions) AS impressions,
     SUM(Clicks) AS clicks,
     ${statement}, 
    (CASE WHEN _PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}' THEN round((SUM(Cost) / 1000000),2) ELSE 0 END ) AS rCost
         FROM
         \`${projectId}.${accountIdNumber}.p_SearchQueryStats_${accountIdNumber}\` WHERE _PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}'
         Group By 1,2,3
    )
    Select
      QueryShort,
      QueriesRaw,
      groupNames,
      CPA,
      totalConversions,
      totalCost,
      recentCost,
      impressions,
      clicks,
      CTR
      FROM(
    Select
      QueryShort,
      QueriesRaw,
      groupNames,
      CPA,
      totalConversions,
      totalCost,
      recentCost,
      impressions,
      clicks,
      CTR
    From(
      SELECT
       TRIM(QueryShort) AS QueryShort,
       ARRAY_AGG(DISTINCT QueryRaw) AS QueriesRaw,
       ARRAY_AGG(DISTINCT label) AS groupNames,
       (CASE WHEN sum(tConversions) > 1 THEN round(((SUM(tCost)) / SUM(tConversions)),2) ELSE round(SUM(tCost),2) END)AS CPA,
       SUM(tConversions) AS totalConversions,
       round(SUM(tCost),2) AS totalCost,
       round(SUM(rCost),2)AS recentCost,
       SUM(impressions) AS impressions,
       SUM(clicks) AS clicks,
       (CASE WHEN sum(clicks) > 1 THEN round(((SUM(clicks)) / SUM(impressions)),4) ELSE 0 END)AS CTR,
        FROM data,
        UNNEST(ML.NGRAMS(SPLIT(REGEXP_REPLACE(LOWER(QueryRaw), r'(\\pP)', r' \\1 '), ' '),
       [1,3],' ')) AS QueryShort 
       Group By 1)) AS main WHERE (main.recentCost > ${t.recentCost} AND  main.CPA > ${t.longCost}) OR main.CPA < ${t.recentCost} AND totalConversions > 3 OR (main.impressions > 1000 AND  main.CTR = 0) ORDER BY recentCost DESC;`
}
export const adCopyTermInsights = (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string, statement:string, type:string, t:any ) => {
  return `    
  WITH data AS  (
  SELECT CreativeId, AdStrengthInfo, JSON_EXTRACT_ARRAY(${type}, '$') AS json_text_string
  FROM \`${projectId}.${accountIdNumber}.p_Ad_${accountIdNumber}\`  WHERE DATE(_PARTITIONTIME) = '${daysEnd}' AND AdType = "RESPONSIVE_SEARCH_AD" AND Status = 'ENABLED' )
  
  SELECT
   nGram,
   '${type}' AS type,
 ARRAY_AGG(DISTINCT CampaignId) AS campaignIds,
 ARRAY_AGG(DISTINCT label) AS groupNames,
 ARRAY_AGG(DISTINCT assetText) AS ads,
 REPLACE(TO_JSON_STRING(ARRAY_AGG(DISTINCT REPLACE( CAST(assetId AS STRING), '"', ''))), '["', '')  AS Id,
 sum( performancePending) AS performancePending,
 sum( performanceGood) AS performanceGood,
 sum( performanceLearning ) AS performanceLearning,
 sum( performanceBest ) AS performanceBest,
 sum( strengthExcellent ) AS strengthExcellent,
 sum( strengthGood) AS strengthGood,
 sum( strengthPoor ) AS strengthPoor,
 round(sum(cost / 1000000),2) AS cost,
 round(sum(conversions),2) AS conversions,
 SUM(impressions) AS impressions,
 SUM(clicks) AS clicks,
 (CASE WHEN sum(clicks) > 1 THEN round(((SUM(clicks)) / SUM(impressions)),4) ELSE 0 END)AS CTR,
FROM(
SELECT
  CampaignId,
  AdStrengthInfo,
  assetPerformanceLabel,
  assetText,
  CAST (assetId AS INT64) AS assetId,
  ${statement},
  (CASE WHEN  REPLACE( assetPerformanceLabel, '"', '') = "PENDING" THEN 1 ELSE 0 END) AS performancePending,
  (CASE WHEN  REPLACE( assetPerformanceLabel, '"', '') = "GOOD" THEN 1 ELSE 0 END) AS performanceGood,
  (CASE WHEN  REPLACE( assetPerformanceLabel, '"', '') = "LEARNING" THEN 1 ELSE 0 END) AS performanceLearning,
  (CASE WHEN  REPLACE( assetPerformanceLabel, '"', '') = "BEST" THEN 1 ELSE 0 END) AS performanceBest,
  (CASE WHEN  REPLACE( AdStrengthInfo, '"', '') = "EXCELLENT" THEN 1 ELSE 0 END) AS strengthExcellent,
  (CASE WHEN  REPLACE( AdStrengthInfo, '"', '') = "GOOD" THEN 1 ELSE 0 END) AS strengthGood,
  (CASE WHEN  REPLACE( AdStrengthInfo, '"', '') = "POOR" THEN 1 ELSE 0 END) AS strengthPoor,
  STRING_AGG(DISTINCT REPLACE(assetText, '"', ''), " ") AS ad,
  sum(cost) AS cost,
  round(sum(conversions),2) AS conversions,
  SUM(impressions) AS impressions,
  SUM(clicks) AS clicks,
FROM(
SELECT e.CampaignId, e.CreativeId, d.AdStrengthInfo,  JSON_EXTRACT(json_text, '$.assetText') AS assetText, JSON_EXTRACT(json_text, '$.assetId') AS assetId,  JSON_EXTRACT(json_text, '$.assetPerformanceLabel') AS assetPerformanceLabel, sum(cost ) AS cost, sum(conversions) AS conversions, SUM(impressions) AS impressions,
SUM(clicks) AS clicks,
FROM data d LEFT JOIN
  \`${projectId}.${accountIdNumber}.p_AdBasicStats_${accountIdNumber}\`  e
   ON
   (e.CreativeId = d.CreativeId) 
    , UNNEST(json_text_string) AS json_text WHERE _PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}'  GROUP BY 1,2,3,4,5,6) AS main GROUP BY 1,2,3,4,5),
    UNNEST(ML.NGRAMS(REGEXP_EXTRACT_ALL((ad ), '[A-z]+'),
        [3 , 30], ' ') ) AS nGram GROUP BY 1 ORDER BY performanceBest DESC`;
}
export const adCopyTermWriter = (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string, statement:string ) => {
  return ` 
  WITH data AS  (SELECT CreativeId,
    c.CreativeFinalUrls AS finalURls,
    c.AdStrengthInfo,
    c.ResponsiveSearchAdPath1,
    c.ResponsiveSearchAdPath2,
    c.ResponsiveSearchAdHeadlines AS rsaHeadlines,
    c.ResponsiveSearchAdDescriptions AS rsaDescriptions,
    cu.Criteria AS keywords,
    cu.SearchPredictedCtr AS predictedCtr, 
    cu.CreativeQualityScore AS adRelevance,                               
    cu.PostClickQualityScore AS landingPageExp,
  FROM 
  \`${projectId}.${accountIdNumber}.p_Ad_${accountIdNumber}\`  c
  LEFT JOIN
  \`${projectId}.${accountIdNumber}.p_Keyword_${accountIdNumber}\` cu
  ON
  (c.CampaignId = cu.CampaignId AND c.AdGroupId = cu.AdGroupId)
  WHERE DATE(c._PARTITIONTIME) =  '${daysEnd}' AND DATE(cu._PARTITIONTIME) =  '${daysEnd}' AND cu.HasQualityScore	= true AND c.AdType = "RESPONSIVE_SEARCH_AD" AND c.Status = 'ENABLED'  AND cu.Status = 'ENABLED' 
  )
  SELECT
 finalUrls,
 CampaignId AS campaignId,
 AdGroupId AS adGroupId,
 CreativeId AS creativeId,
 AdStrengthInfo AS adStrength,
 label AS groupNames,
  ResponsiveSearchAdPath1 AS path1,
  ResponsiveSearchAdPath2 AS path2,
 rsaDescriptions AS rsaDescriptions,
 rsaHeadlines rsaHeadlines, 
 keywords,
 predictedCtr,
 adRelevance,  
 landingPageExp,
 ARRAY_AGG( keywordGram) AS keywordGram,
 round(sum(cost / 1000000),2) AS cost,
 round(sum(conversions),2) AS conversions,
 SUM(impressions) AS impressions,
 SUM(clicks) AS clicks
FROM(
SELECT
  REPLACE(REPLACE(finalUrls, '["', ''), '"]', '') AS finalUrls,
  CampaignId,
  AdGroupId,
  CreativeId,
  ResponsiveSearchAdPath1,
  ResponsiveSearchAdPath2,
  AdStrengthInfo,
  rsaHeadlines, 
  rsaDescriptions,
  TO_JSON_STRING( ARRAY_AGG(  predictedCtr )) AS  predictedCtr,
  TO_JSON_STRING( ARRAY_AGG(  adRelevance )) AS adRelevance,
  TO_JSON_STRING( ARRAY_AGG(  landingPageExp )) AS landingPageExp,
  TO_JSON_STRING( ARRAY_AGG( DISTINCT keywords )) AS keywords,
  ${statement},
  sum(cost) AS cost,
  round(sum(conversions),2) AS conversions,
  SUM(impressions) AS impressions,
  SUM(clicks) AS clicks,
FROM(
  SELECT e.CampaignId, e.AdGroupId, e.CreativeId, d.ResponsiveSearchAdPath1,
  d.ResponsiveSearchAdPath2, d.finalUrls, d.AdStrengthInfo, d.rsaHeadlines, d.rsaDescriptions,  d.predictedCtr,
  d.adRelevance,  
  d.landingPageExp,  d.keywords AS keywords, sum(cost ) AS cost, sum(conversions) AS conversions, SUM(impressions) AS impressions,
  SUM(clicks) AS clicks,
FROM data d LEFT JOIN
  \`${projectId}.${accountIdNumber}.p_AdBasicStats_${accountIdNumber}\`  e
   ON
   (e.CreativeId = d.CreativeId)  WHERE _PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}'  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13)  AS main GROUP BY 1,2,3,4,5,6,7,8,9) , UNNEST(ML.NGRAMS(REGEXP_EXTRACT_ALL((keywords ), '[A-z]+'),
   [1 , 3], ' ') ) AS keywordGram WHERE impressions > 100 GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14 ORDER BY impressions desc `;
}
export const adCopyTermInsightsExpanded = (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string, statement:string, type:string, t:any ) => {
  return `    
  WITH data AS  (
  SELECT CreativeId, AdStrengthInfo, ${type} AS json_text
  FROM \`${projectId}.${accountIdNumber}.p_Ad_${accountIdNumber}\`  WHERE DATE(_PARTITIONTIME) = '${daysEnd}' AND AdType = "EXPANDED_TEXT_AD" AND Status = 'ENABLED' )

   SELECT
   nGram,
   '${type}' AS type,
 ARRAY_AGG(DISTINCT CampaignId) AS campaignIds,
 ARRAY_AGG(DISTINCT label) AS groupNames,
 ARRAY_AGG(DISTINCT assetText) AS ads,
 TO_JSON_STRING(ARRAY_AGG(DISTINCT REPLACE( CAST(CreativeId AS STRING), '"', ''))) AS Id,
 0 AS performancePending,
 0 AS performanceGood,
 0 AS performanceLearning,
 0 AS performanceBest,
 0 AS strengthExcellent,
 0 AS strengthGood,
 0 AS strengthPoor,
 round(sum(cost / 1000000),2) AS cost,
 round(sum(conversions),2) AS conversions,
 SUM(impressions) AS impressions,
 SUM(clicks) AS clicks,
 (CASE WHEN sum(clicks) > 1 THEN round(((SUM(clicks)) / SUM(impressions)),4) ELSE 0 END)AS CTR,
FROM(
SELECT
  CampaignId,
  CreativeId,
  json_text AS assetText,
  ${statement},
  STRING_AGG(DISTINCT REPLACE(json_text, '"', ''), " ") AS ad,
  sum(cost) AS cost,
  round(sum(conversions),2) AS conversions,
  SUM(impressions) AS impressions,
 SUM(clicks) AS clicks
FROM(
SELECT e.CampaignId, e.CreativeId, d.json_text, sum(cost ) AS cost, sum(conversions) AS conversions, SUM(impressions) AS impressions,
SUM(clicks) AS clicks,
  FROM data d LEFT JOIN
  \`${projectId}.${accountIdNumber}.p_AdBasicStats_${accountIdNumber}\`  e
   ON
   (e.CreativeId = d.CreativeId) 
    WHERE _PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}'  GROUP BY 1,2,3) AS main GROUP BY 1,2,3),
    UNNEST(ML.NGRAMS(REGEXP_EXTRACT_ALL((ad ), '[A-z]+'),
        [3 , 30], ' ') ) AS nGram GROUP BY 1`;
}

export const queryGroupsSuggest = (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string, campaignIdArray:any ) => {
const regex1 = " r'(\\pP)', r' \\1 '), ' '),[1,6],' ')"
const regex2 = " r'(\\pP)', r' \\1 '), ' '),[1,1],' ')"
  return `
  WITH data AS  (
    SELECT
   LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REGEXP_REPLACE((c.CampaignName), '[^0-9A-Za-z_ ]',  ' '), '_', ' '),'  ', ' '),'  ', ' '))) AS CampaignName, 
   c.CampaignId AS ID,
   c.AdvertisingChannelType,
   (CASE WHEN cs.IsRestrict is null THEN false ELSE cs.IsRestrict END) AS IsRestrict,
   FROM 
   \`${projectId}.${accountIdNumber}.p_Campaign_${accountIdNumber}\` c
   LEFT JOIN
   \`${projectId}.${accountIdNumber}.p_Audience_${accountIdNumber}\`cs
      ON
       (c.CampaignId = cs.CampaignId) 
   WHERE c._PARTITIONDATE BETWEEN '${daysEnd}' AND '${daysEnd}'   
   AND CAST(c.CampaignId AS STRING) IN UNNEST(${JSON.stringify(campaignIdArray)})
   GROUP BY 1,2,3,4
   )
   
   SELECT
      IDs,
      type,
      network,
      CASE WHEN regexp_CONTAINS((audience), 'true') THEN true ELSE false END AS audience,
      REPLACE(REGEXP_REPLACE(STRING_AGG(CampaignNameContains), (STRING_AGG(NAMES,"|")) ,"" ), '","","', '","')  AS possibleNames,
      STRING_AGG((Names) ORDER BY LENGTH(Names) LIMIT 1) AS name,
      ARRAY_AGG(DISTINCT( campaignNames)) AS campaignNames,
      STRING_AGG(DISTINCT(Names)) AS aka,
      CASE WHEN sum(DSACount) > 0 THEN true ELSE false END AS dsa,
      CASE WHEN sum(SearchCount) > 0 THEN true ELSE false END AS search, 
      CASE WHEN regexp_CONTAINS((type), 'DISPLAY') THEN true ELSE false END AS display,
      CASE WHEN regexp_CONTAINS((network), 'MIXED') THEN true ELSE false END AS mixed
        
     FROM (
     SELECT 
     REPLACE(LTRIM(RTRIM(CAST(Query AS STRING))), '  ', ' ') AS Names,
     STRING_AGG(DISTINCT TO_JSON_STRING (AdvertisingChannelType)) AS type,
     STRING_AGG(DISTINCT TO_JSON_STRING (AdNetworkType1)) AS network,
     STRING_AGG(DISTINCT TO_JSON_STRING (IsRestrict)) AS audience,
     STRING_AGG(DISTINCT TO_JSON_STRING(cs.CampaignId)) AS IDs,
     STRING_AGG(DISTINCT TO_JSON_STRING(CampaignName)) AS campaignNames,
     STRING_AGG(DISTINCT TO_JSON_STRING(SingleQuery)) AS CampaignNameContains,
     COUNT(DISTINCT ID ) AS CampaignCount,
     SUM(cs.Impressions) AS Impressions,
     SUM(cs.Clicks) AS Clicks,
     SUM(CASE WHEN ca.AdGroupType = "SEARCH_DYNAMIC_ADS" THEN 1 ELSE 0 END) AS DSACount,
     SUM(CASE WHEN ca.AdGroupType = "SEARCH_STANDARD" THEN 1 ELSE 0 END) AS SearchCount, 
     FROM data
     LEFT JOIN
     \`${projectId}.${accountIdNumber}.p_CampaignBasicStats_${accountIdNumber}\` cs
       ON
       (ID = cs.CampaignId)
       INNER JOIN
       \`${projectId}.${accountIdNumber}.p_AdGroup_${accountIdNumber}\` ca
           ON
       (ID = ca.CampaignId) ,
       UNNEST(ML.NGRAMS(SPLIT(REGEXP_REPLACE((CampaignName),  ${regex1} 
      ) AS Query,  
       UNNEST(ML.NGRAMS(SPLIT(REGEXP_REPLACE((CampaignName),  ${regex2} 
      ) AS SingleQuery  
      WHERE ca._PARTITIONDATE BETWEEN '${daysEnd}' AND '${daysEnd}' AND cs._PARTITIONDATE BETWEEN '${daysEnd}' AND '${daysEnd}'
       GROUP BY 1) WHERE CampaignCount > 0 AND Names != ""  GROUP BY 1,2,3,4
  `;

}

export const adDisapprovals = (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string) => {
  return `SELECT 
  date, campaignId, campaignName, adGroupId,adGroupName,adId, adType, approvalStatus, JSON_EXTRACT_STRING_ARRAY(policySummary) AS note, JSON_EXTRACT_STRING_ARRAY(urls) AS url
FROM (
  SELECT CAST(Min(c._PARTITIONDATE) AS STRING) AS date, c.CampaignId AS campaignId, ct.CampaignName AS campaignName, c.AdGroupId AS adGroupId, AdGroupName AS adGroupName, CreativeId AS adId, Status AS status, AdType As adType, CombinedApprovalStatus As approvalStatus, 
 c.PolicySummary AS policySummary, c.CreativeFinalUrls AS urls

  FROM \`${projectId}.${accountIdNumber}.p_Ad_${accountIdNumber}\` c

    LEFT JOIN
    \`${projectId}.${accountIdNumber}.p_AdGroup_${accountIdNumber}\` cs
    ON
    (c.CampaignId = cs.CampaignId AND c.AdGroupId = cs.AdGroupId)
    
    LEFT JOIN
    \`${projectId}.${accountIdNumber}.p_Campaign_${accountIdNumber}\` ct
    ON
    (c.CampaignId = ct.CampaignId)
 
WHERE CombinedApprovalStatus = "disapproved" AND Status = "ENABLED" AND CampaignStatus = "ENABLED" AND AdGroupStatus = "ENABLED" AND c._PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}' AND cs._PARTITIONDATE = '${daysEnd}' AND ct._PARTITIONDATE = '${daysEnd}'  GROUP BY 
2,3,4,5,6,7,8,9,10,11) As basic `;

}

export const urlInsights = (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string) => {
  return ` WITH data AS  (
  SELECT CreativeFinalUrls AS urls, CONCAT(CAST(cs.campaignId as STRING),'-',CAST(cs.AdGroupId as STRING)) AS ID, CONCAT(CAST(ct.campaignName as STRING),'-',CAST(cs.AdGroupName as STRING)) AS name, sum(cd.Impressions) AS Impressions,  sum(cd.Clicks) AS Clicks
  FROM  
  \`${projectId}.${accountIdNumber}.p_Ad_${accountIdNumber}\` c
  LEFT JOIN
  \`${projectId}.${accountIdNumber}.p_AdGroup_${accountIdNumber}\` cs
  ON
  (c.CampaignId = cs.CampaignId AND c.AdGroupId = cs.AdGroupId)
  
  LEFT JOIN
  \`${projectId}.${accountIdNumber}.p_Campaign_${accountIdNumber}\` ct
  ON
  (c.CampaignId = ct.CampaignId)

  LEFT JOIN
  \`${projectId}.${accountIdNumber}.p_AdBasicStats_${accountIdNumber}\` cd
 ON
 (c.CampaignId = cd.CampaignId AND c.AdGroupId = cd.AdGroupId)

   WHERE CreativeFinalUrls IS NOT NULL AND CreativeFinalUrls NOT LIKE 'www.youtube.com/watch' AND c.Status = "ENABLED" AND ct.CampaignStatus = "ENABLED" AND cs.AdGroupStatus = "ENABLED" AND c._PARTITIONDATE = '${daysEnd}' AND cs._PARTITIONDATE = '${daysEnd}' AND ct._PARTITIONDATE = '${daysEnd}' AND  cd._PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}' AND cd.Impressions > 0 GROUP By 1,2,3
  )
  
  SELECT
      urls,
      "Ad" AS type,
      JSON_EXTRACT_ARRAY(urls) AS link,
      STRING_AGG(DISTINCT ID) AS ID,
      STRING_AGG(DISTINCT name) AS name,
      sum(Impressions) AS impressions,
      sum(Clicks) AS clicks
      FROM data 
      GROUP By 1
 
    `;
  }

  export const pMaxInsightsSuggestions = (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string, campaignIds:any) => {
    return `SELECT CampaignId AS campaignId, (CASE WHEN Device = "CONNECTED_TV" THEN "Mixed" WHEN ClickType = "URL_CLICKS" THEN "Search" WHEN ClickType = "PRODUCT_LISTING_AD_CLICKS" THEN "Shopping" 
    WHEN ClickType = "LOCATION_EXPANSION" THEN "Maps" 
    WHEN ClickType = "GET_DIRECTIONS"  THEN "Maps" 
    WHEN ClickType = "CALLS"  THEN "Phone Calls" 
    WHEN ClickType = "SITELINKS" THEN "Search"  ELSE "Mixed" END) AS trafficType, sum(Clicks) AS clicks
   FROM \`${projectId}.${accountIdNumber}.p_ClickStats_${accountIdNumber}\`
   WHERE DATE(_PARTITIONDATE) BETWEEN '${daysStart}' AND '${daysEnd}' AND
    CampaignId IN UNNEST(${JSON.stringify(campaignIds)}) GROUP BY 1,2
  `;
}

export const pMaxCampaigns = (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string, campaignIds:any) => {
  return `SELECT CampaignId AS campaignId, 
        sum(Impressions) AS totalImpressions,
        sum(Clicks) AS totalClicks
      FROM \`${projectId}.${accountIdNumber}.p_CampaignBasicStats_${accountIdNumber}\`
      WHERE DATE(_PARTITIONDATE) BETWEEN '${daysStart}' AND '${daysEnd}' AND
      CampaignId IN UNNEST(${JSON.stringify(campaignIds)})
      GROUP BY 1
  `;
}

 