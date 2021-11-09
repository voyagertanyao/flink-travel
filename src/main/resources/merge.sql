insert overwrite table test.ods_package_info partition (dt = '2021-11-08')
select (case when delta.`_op` is null then snap.id else delta.id end)                                   as id,
       (case when delta.`_op` is null then snap.code else delta.code end)                               as code,
       (case when delta.`_op` is null then snap.name else delta.name end)                               as name,
       (case when delta.`_op` is null then snap.channel else delta.channel end)                         as channel,
       (case when delta.`_op` is null then snap.cost else delta.cost end)                               as cost,
       (case when delta.`_op` is null then snap.description else delta.description end)                 as description,
       (case when delta.`_op` is null then snap.month_num else delta.month_num end)                     as month_num,
       (case
            when delta.`_op` is null then snap.reserve_order_show
            else delta.reserve_order_show end)                                                          as reserve_order_show,
       (case
            when delta.`_op` is null then snap.reserve_change_show
            else delta.reserve_change_show end)                                                         as reserve_change_show,
       (case
            when delta.`_op` is null then snap.reserve_alipay_show
            else delta.reserve_alipay_show end)                                                         as reserve_alipay_show,
       (case
            when delta.`_op` is null then snap.package_title
            else delta.package_title end)                                                               as package_title,
       (case
            when delta.`_op` is null then snap.storage_attribution
            else delta.storage_attribution end)                                                         as storage_attribution,
       (case
            when delta.`_op` is null then snap.auto_renew_price
            else delta.auto_renew_price end)                                                            as auto_renew_price,
       (case when delta.`_op` is null then snap.create_time else delta.create_time end)                 as create_time,
       (case when delta.`_op` is null then snap.update_time else delta.update_time end)                 as update_time,
       (case when delta.`_op` is null then snap.platform else delta.platform end)                       as platform,
       (case
            when delta.`_op` is null then snap.auto_renew_type
            else delta.auto_renew_type end)                                                             as auto_renew_type,
       (case when delta.`_op` is null then snap.project_code else delta.project_code end)               as project_code
from (
         SELECT *
         from test.ods_package_info
         where dt = '2021-11-07'
     ) as snap
         full join
     (
         SELECT *
         FROM (SELECT row_number() over (PARTITION BY id
             ORDER BY `_pos` DESC, `_sec` DESC) AS rn,
                      *
               FROM test.ods_package_info_delta
               WHERE `_op` != 'SNAPSHOT') AS rtable
         WHERE rtable.rn = 1
     ) as delta on delta.id = snap.id
where delta.`_op` is null
   or delta.`_op` != 'DELETE'
