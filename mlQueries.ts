// where all ML SQL Queries are stored

export const queryTrain = function (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string, model: any, modelInfo:any) {
    return ` CREATE OR REPLACE MODEL \`${projectId}.${accountIdNumber}.${model}\`
    OPTIONS
      (${modelInfo}) AS
  `; // base is added at end
}
export const queryEvaluate = function (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string, model: string) { 
  return ` SELECT
  *
FROM
  ML.EVALUATE(MODEL \`${projectId}.${accountIdNumber}.${model}\`
  `; // base is added at end
}
export const queryEvaluatePacing = function (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string, model: string, groupName:string, statement:string) { 
  return `Select
            ABS( AVG(avgDiff)) AS score,
              FROM(
              Select
              predict.date,
              round(predict.cost) AS predictCost,
              actual.cost AS actualCost,
              (CASE WHEN actual.cost > 1 AND predict.cost > 1 THEN round((100 / (predict.cost )) * (actual.cost)) ELSE 0 END) AS avgDiff
            FROM (Select
              FORMAT_TIMESTAMP("%Y-%m-%d", TIMESTAMP (CAST(forecast_timestamp AS STRING))) AS date,
              round((forecast_value)) AS cost,
              FROM
              ML.FORECAST(MODEL \`${projectId}.${accountIdNumber}.${model}\` ,
                          STRUCT(31 AS horizon, 0.8 AS confidence_level))
              WHERE label = '${groupName}' AND forecast_timestamp > timestamp_sub(TIMESTAMP( '${daysStart}' ), INTERVAL 1 DAY) AND forecast_timestamp < timestamp_add(TIMESTAMP('${daysEnd}'), INTERVAL 31 DAY)  
                          ORDER BY date ASC) AS predict
            LEFT JOIN  
            (SELECT
              date AS date,
              Cast(round((SUM(Cost) / 1000000))AS INT64) AS cost,
              "Account" AS label
            FROM
            \`${projectId}.${accountIdNumber}.p_AccountBasicStats_${accountIdNumber}\`
             WHERE _PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}' GROUP BY 1
             
             UNION ALL  
             
             SELECT
              date AS date,
              Cast(round((SUM(Cost) / 1000000))AS INT64) AS cost,
              ${statement}
            FROM
            \`${projectId}.${accountIdNumber}.p_CampaignBasicStats_${accountIdNumber}\`
             WHERE _PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}'  GROUP BY 1,3
             ) AS actual
                ON
                    (predict.date = CAST(DATE(actual.date)AS STRING)) WHERE label = '${groupName}' ) AS cal`; 
}

export const queryFeatureInfo = function (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string, model: any) { 
  return ` SELECT
  *
FROM
  ML.FEATURE_INFO(MODEL \`${projectId}.${accountIdNumber}.${model}\`)`;
}

export const queryFeatureImportance = function (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string, model: any) { 
  return ` SELECT
  *
FROM
  ML.FEATURE_IMPORTANCE(MODEL \`${projectId}.${accountIdNumber}.${model}\`)`;
}

export const queryExplain = function (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string, model: any) { 
  return ` SELECT
  *
FROM
  ML.GLOBAL_EXPLAIN(MODEL \`${projectId}.${accountIdNumber}.${model}\`)`;
}

export const queryExplainPredict = function (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string, model: any) { 
  return ` SELECT
  *
FROM
  ML.EXPLAIN_PREDICT(MODEL \`${projectId}.${accountIdNumber}.${model}\`,
  (
  `; // base is added at end
}

export const queryPacingBase = function (projectId:string, accountIdNumber: number, daysStart: string, daysEnd: string, statement : string ) {  
  return `SELECT
            date AS parsed_date,
            Cast(round((SUM(Cost) / 1000000))AS INT64) AS cost,
            "Account" AS label
          FROM
          \`${projectId}.${accountIdNumber}.p_AccountBasicStats_${accountIdNumber}\`
           WHERE _PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}' GROUP BY 1
           
           UNION ALL  
           
           SELECT
            date AS parsed_date,
            Cast(round((SUM(Cost) / 1000000))AS INT64) AS cost,
            ${statement}
          FROM
          \`${projectId}.${accountIdNumber}.p_CampaignBasicStats_${accountIdNumber}\`
           WHERE _PARTITIONDATE BETWEEN '${daysStart}' AND '${daysEnd}'  GROUP BY 1,3
           `;
}

export const queryPacingPredict = function (projectId:string, accountIdNumber: number, daysLeft: string, daysStart: string, daysEnd: string, groupName: string) {    
    return `SELECT
 FORMAT_TIMESTAMP("%Y-%m-%d", TIMESTAMP (CAST(forecast_timestamp AS STRING))) AS date,
 round(forecast_value) AS cost,
FROM
 ML.FORECAST(MODEL  \`${projectId}.${accountIdNumber}.timeseries_account\`,
             STRUCT(31 AS horizon, 0.8 AS confidence_level)) WHERE label = '${groupName}' AND forecast_timestamp > timestamp_sub(TIMESTAMP('${daysEnd}'), INTERVAL 1 DAY) AND forecast_timestamp < timestamp_add(TIMESTAMP('${daysEnd}'), INTERVAL ${daysLeft} DAY)
             ORDER BY date ASC`;
}



