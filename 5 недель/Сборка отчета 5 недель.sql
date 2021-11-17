-- основной блок, содержит выборки товаров, продававшихся или находившихся на складах за заданный период

@set date_x = '2021-06-11'
@set date_first = '2021-05-07'
@set date_last = '2021-06-10'
@set date_stfirst = '2021-05-08'
@set date_stlast = '2021-06-12'


SELECT ${date_x}::DATE, ${date_x}::TIMESTAMP WITHOUT TIME ZONE

SELECT ${date_stlast}::DATE, ${date_stlast}::TIMESTAMP WITHOUT TIME ZONE

/* базовая версия основного блока данных */

select *
from (
	select t1.feature_id, t1.group_name, t1.subgroup_name, t1.category_name, t1.subcategory_name, t1.model_id, t2.modelname, t1.code as code, t1.color, 'http://am.look.online/storage/images/'+t2.uuid+'.jpg' as foto, t1.size, 'црм' as cm_crm 
	from (
		select t1.feature_id, t1.model_id, t1.color, t1."size", t1.firstprice, t2.group_name, t2.subgroup_name, t2.category_name, t2.subcategory_name, t1.firstcolorcode, t1.code              /* добавляем иерархию*/
		from (
			select t1.*, t2."name" as "size"
			from (
				select t1.*, t2."name" as color
				from (
					select feature_id, model_id, color_id, size_id, firstcolorcode, max (firstprice) as firstprice, code
						from (
							select t1.*, t2.model_id, t2.color_id, t2.size_id, t2.firstprice, firstcolorcode, t2.code 
							from 
								(
									select * from 
									(
										SELECT DISTINCT feature_id FROM public.sales where datetime between ${date_first}::DATE and ${date_x}::DATE
										union
										SELECT DISTINCT feature_id FROM stocks where datetime between ${date_stfirst}::DATE and ${date_stlast}::DATE and quantity <> 0
									) group by feature_id 
								) as t1 left join features as t2 on t1.feature_id = t2.feature_id
						) group by model_id, feature_id, color_id, size_id, firstcolorcode, code
					) as t1 left join colors as t2 on t1.color_id = t2.color_id	
				) as t1 left join sizes as t2 on t1.size_id = t2.size_id	
			) as t1 left join persisted_hierarchy as t2 on t1.model_id = t2.model_id	
	) as t1 left join models as t2 on t1.model_id = t2.model_id
) where model_id <> '09048' and group_name <> '4. Подарочные сертификаты' ORDER by group_name, subgroup_name, category_name, subcategory_name, modelname, color, size



/* версия с синтаксисом by Спиренков */
/* текущий рабочий вариант */

select m.modelname
, 'http://am.look.online/storage/images/'+uuid+'.jpg' as foto
, '' as fullmodelcode
, '' as colorcode
, group_id
, subgroup_id
, category_id
, subcategory_id
, m.model_id
, status
, m.season
, c."name" as color
, trim(m.model_id)+';'+c."name" as service
, min as first_date
, '1' as cm
, codes
from (
	select * from 
	(
	SELECT DISTINCT feature_id FROM public.sales where datetime between ${date_first}::DATE and ${date_x}::DATE
	union
	SELECT DISTINCT feature_id FROM stocks where datetime between ${date_stfirst}::DATE and ${date_stlast}::DATE and quantity <> 0
	) group by feature_id 
	) as b
join features f on b.feature_id = f.feature_id
join colors c on f.color_id = c.color_id 
join persisted_hierarchy ph on f.model_id = ph.model_id
join models m on f.model_id = m.model_id
join ( 
		select model_id, color, CAST(listagg(code, ', ') within group (order by code) AS VARCHAR(200)) as codes
		from
		(
		select t1.model_id, t1.code, t2."name" as color from features  as t1 left join colors as t2 on t1.color_id = t2.color_id
		) group by model_id, color
	) cc on f.model_id = cc.model_id and c."name" = cc.color
left join block_first_selling_date bfsd on f.model_id = bfsd.model_id and c."name" = bfsd.color

select
	*
from
	(
	select
		m.modelname ,
		'http://am.look.online/storage/images/' + uuid + '.jpg' as foto ,
		'' as colorcode ,
		'' as fullmodelcode ,
		m.model_id ,
		group_name as group ,
		subgroup_name as subgroup ,
		category_name as category ,
		subcategory_name as subcategory ,
		status ,
		m.season ,
		c."name" as colorname ,
		trim(m.model_id)+ ';' + c."name" as service ,
		min as first_date ,
		'1' as cm ,
		codes
	from
		(
		select
			*
		from
			(
			SELECT
				DISTINCT feature_id
			FROM
				public.sales
			where
				datetime between '2021-05-13' and '2021-06-17'
		union
			SELECT
				DISTINCT feature_id
			FROM
				stocks
			where
				datetime between '2021-05-14' and '2021-06-18'
				and quantity <> 0 )
		group by
			feature_id ) as b
	join features f on
		b.feature_id = f.feature_id
	join colors c on
		f.color_id = c.color_id
	join persisted_hierarchy ph on
		f.model_id = ph.model_id
	join models m on
		f.model_id = m.model_id
	join (
		select
			model_id,
			color,
			CAST(listagg(code, ', ') within group (order by code) AS VARCHAR(200)) as codes
		from
			(
			select
				t1.model_id,
				t1.code,
				t2."name" as color
			from
				features as t1
			left join colors as t2 on
				t1.color_id = t2.color_id )
		group by
			model_id,
			color ) cc on
		f.model_id = cc.model_id
		and c."name" = cc.color
	left join block_first_selling_date bfsd on
		f.model_id = bfsd.model_id
		and c."name" = bfsd.color )
where
	"group" <> '4. Подарочные сертификаты'
	and "group" <> '5. Тест'
	
	select
		m.modelname ,
		'http://am.look.online/storage/images/' + uuid + '.jpg' as foto ,
		'' as colorcode ,
		'' as fullmodelcode ,
		m.model_id ,
		group_name as group ,
		subgroup_name as subgroup ,
		category_name as category ,
		subcategory_name as subcategory ,
		status ,
		m.season ,
		c."name" as colorname ,
		trim(m.model_id)+ ';' + c."name" as service ,
		min as first_date ,
		'1' as cm ,
		codes
	from
		(
		select
			*
		from
			(
			SELECT
				DISTINCT feature_id
			FROM
				public.sales
			where
				datetime between '2021-05-13' and '2021-06-17'
		union
			SELECT
				DISTINCT feature_id
			FROM
				stocks
			where
				datetime between '2021-05-14' and '2021-06-18'
				and quantity <> 0 )
		group by
			feature_id ) as b
	join features f on
		b.feature_id = f.feature_id
	join colors c on
		f.color_id = c.color_id
	join persisted_hierarchy ph on
		f.model_id = ph.model_id
	join models m on
		f.model_id = m.model_id
	join (
		select
			model_id,
			color,
			CAST(listagg(code, ', ') within group (order by code) AS VARCHAR(200)) as codes
		from
			(
			select
				t1.model_id,
				t1.code,
				t2."name" as color
			from
				features as t1
			left join colors as t2 on
				t1.color_id = t2.color_id )
		group by
			model_id,
			color ) cc on
		f.model_id = cc.model_id
		and c."name" = cc.color
	left join block_first_selling_date bfsd on
		f.model_id = bfsd.model_id
		and c."name" = bfsd.color
		
		
		
select * from  (     select trim(m.model_id) as model_id     , 'http://am.look.online/storage/images/'+uuid+'.jpg' as foto     , '' as colorcode     , '' as fullmodelcode     , m.model_id     , group_name as group     , subgroup_name as subgroup     , category_name as category     , subcategory_name as subcategory     , status     , m.season     , c."name" as colorname     , trim(m.model_id)+';'+c."name" as service     , min as first_date     , '1' as cm     , codes     from (         select * from          (         SELECT DISTINCT feature_id FROM public.sales where datetime between '2021-05-13' and '2021-06-17'         union         SELECT DISTINCT feature_id FROM stocks where datetime between '2021-05-14' and '2021-06-18' and quantity <> 0         ) group by feature_id          ) as b     join features f on b.feature_id = f.feature_id     join colors c on f.color_id = c.color_id      join persisted_hierarchy ph on f.model_id = ph.model_id     join models m on f.model_id = m.model_id     join (              select model_id, color, CAST(listagg(code, ', ') within group (order by code) AS VARCHAR(200)) as codes             from             (             select t1.model_id, t1.code, t2."name" as color from features  as t1 left join colors as t2 on t1.color_id = t2.color_id             ) group by model_id, color         ) cc on f.model_id = cc.model_id and c."name" = cc.color     left join block_first_selling_date bfsd on f.model_id = bfsd.model_id and c."name" = bfsd.color ) where "group" <> '4. Подарочные сертификаты' and "group" <> '5. Тест'