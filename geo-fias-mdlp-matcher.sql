DROP TABLE if exists #BaseQuery;
DROP TABLE if exists #SplitPaths;
DROP TABLE if exists #temp;
DROP TABLE if exists #level10;
DROP TABLE if exists #gar;
DROP TABLE if exists #geo_data;
DROP TABLE if exists #mdlp_isnull;
DROP TABLE if exists #one_fias_rns;
DROP TABLE if exists #Final_1_code_Fias;
DROP TABLE if exists #null_house;
DROP TABLE if exists #distinctID;
DROP TABLE if exists #Final_code_Fias;




--Обращаемся к реестру объектов ГАР (AS_REESTR_OBJECTS) и достаем для каждого объекта его составные части (PATH) из (AS_MUN_HIERARCHY)
SELECT 
        obj.OBJECTID,
        obj.OBJECTID AS OBJECTID_ish,
        obj.OBJECTGUID,
        obj.CHANGEID,
        obj.LEVELID,
        obj.CREATEDATE,
        obj.UPDATEDATE,
        obj.ISACTIVE,
        mun.PATH
		INTO #BaseQuery
    FROM FIAS.dbo.AS_REESTR_OBJECTS AS obj
    JOIN FIAS.dbo.AS_MUN_HIERARCHY AS mun 
        ON obj.OBJECTID = mun.OBJECTID
    WHERE 
      obj.LEVELID >= 8
	  and mun.ISACTIVE='1'

--Обращаемся к BaseQuery, чтобы развернуть PATH в строки с указанием ссылки на исходный объект
SELECT DISTINCT
        bq.OBJECTID AS BaseOBJECTID,
        bq.OBJECTID_ish,
        CAST(LTRIM(RTRIM(s.value)) AS INT) AS SplitOBJECTID 
		INTO #SplitPaths
    FROM #BaseQuery bq
    CROSS APPLY STRING_SPLIT(bq.PATH, '.') s

--Вновь обращаемся к ресстру объектов, чтобы достать ключевые данные, дополнительно обращаемся к справочнику адресных объектов, достаем тип и название
SELECT 
    sp.SplitOBJECTID AS OBJECTID,
    sp.OBJECTID_ish,
    re.OBJECTGUID,
    re.CHANGEID,
    re.LEVELID,
    re.CREATEDATE,
    re.UPDATEDATE,
    re.ISACTIVE,
    addr.NAME,
    addr.TYPENAME
INTO #temp
FROM #SplitPaths sp
JOIN  FIAS.dbo.AS_REESTR_OBJECTS re 
    ON sp.SplitOBJECTID = re.OBJECTID
LEFT JOIN FIAS.dbo.AS_ADDR_OBJ addr
    ON re.OBJECTGUID = addr.OBJECTGUID
    AND addr.ISACTIVE = 1;

--Формируем справочник объектов >=10 уровню и для каждого указываем его код фиас level=10, т.к только в нем содержится ссылка на номер дома.
Select distinct lv10.OBJECTGUID,lv11.OBJECTGUID as OBJECTGUIDLv10
into #level10
From #temp lv11 left join #temp lv10 on lv11.OBJECTID_ish=lv10.OBJECTID_ish and lv11.LEVELID=10
Where lv10.LEVELID>=10

-- В таблице temp создаем индекс для ускорения JOIN
CREATE CLUSTERED INDEX IX_temp_OBJECTID_ish ON #temp (OBJECTID_ish, LEVELID);

-- Для каждого исходного объекта достаем его составляющие
SELECT 
    t.OBJECTID,
    t.OBJECTID_ish,
    t.OBJECTGUID,
    t.ISACTIVE,
    t.LEVELID,
    subj.TYPENAME + ' ' + subj.NAME AS Субъект,
    avtO.TYPENAME + ' ' + avtO.NAME AS Автономный_окр,
    MunR.TYPENAME + ' ' + MunR.NAME AS Район,
    City.TYPENAME + ' ' + City.NAME AS Город,
    Vntr.TYPENAME + ' ' + Vntr.NAME AS Внутригородская_тер,
    Nasp.TYPENAME + ' ' + Nasp.NAME AS Населенный_пункт,
	Str.TYPENAME + ' ' + Str.NAME AS Улица,
	snt.TYPENAME + ' ' + Snt.NAME AS СНТ,
	ht.SHORTNAME + ' ' + house.HOUSENUM as Дом
	into #gar
FROM #temp t
JOIN (
    SELECT OBJECTID_ish, MAX(LEVELID) AS MaxLevel
    FROM #temp
    GROUP BY OBJECTID_ish
) max_levels 
ON t.OBJECTID_ish = max_levels.OBJECTID_ish 
AND t.LEVELID = max_levels.MaxLevel
LEFT JOIN #temp subj ON t.OBJECTID_ish = subj.OBJECTID_ish AND subj.LEVELID = 1
LEFT JOIN #temp avtO ON t.OBJECTID_ish = avtO.OBJECTID_ish AND avtO.LEVELID = 2
LEFT JOIN #temp MunR ON t.OBJECTID_ish = MunR.OBJECTID_ish AND MunR.LEVELID = 3
LEFT JOIN #temp City ON t.OBJECTID_ish = City.OBJECTID_ish AND City.LEVELID = 4
LEFT JOIN #temp Vntr ON t.OBJECTID_ish = Vntr.OBJECTID_ish AND Vntr.LEVELID = 5	
LEFT JOIN #temp Nasp ON t.OBJECTID_ish = Nasp.OBJECTID_ish AND Nasp.LEVELID = 6
LEFT JOIN #temp Snt ON t.OBJECTID_ish = Snt.OBJECTID_ish AND Snt.LEVELID = 7
LEFT JOIN #temp Str ON t.OBJECTID_ish = str.OBJECTID_ish AND str.LEVELID = 8
LEFT JOIN #level10 l10 on l10.OBJECTGUID=t.OBJECTGUID
LEFT JOIN [FIAS].[dbo].[AS_HOUSES] house on l10.OBJECTGUIDLv10 = house.OBJECTGUID and house.ISACTIVE=1
LEFT JOIN [FIAS].[dbo].[AS_HOUSE_TYPES] HT on HT.ID=house.HOUSETYPE

--Подтягиваем полученный результат по коду ФИАС МДЛП и коду ФИАС на точке из сенсуса
SELECT 
        rtl_hash.branch_address_resolved_address,
        sen.[Адрес РНС],
        g.Внутригородская_тер AS Фиас_lev5,
        g.Населенный_пункт AS Фиас_lv6,
        g.Город AS Фиас_lv4,
        g.Район AS Фиас_lv3,
        g.Автономный_окр AS Фиас_Lv2,
        g.Субъект AS Фиас_Lv1,
		g.СНТ as Фиас_Lv7,
		g.улица as Фиас_Lv8,
		g.дом as Фиас_Дом,

        g2.Внутригородская_тер AS ФиасСен_lev5,
        g2.Населенный_пункт AS ФиасСен_lv6,
        g2.Город AS ФиасСен_lv4,
        g2.Район AS ФиасСен_lv3,
        g2.Автономный_окр AS ФиасСен_Lv2,
        g2.Субъект AS ФиасСен_Lv1,
		g2.СНТ as ФиасСен_Lv7,
		g2.улица as ФиасСен_Lv8,
		g2.дом as ФиасСен_Дом,

        a.[ID РНС],
        rtl_hash.branch_address_fias_houseguid,
        sen.[Код ФИАС],

        COALESCE(
            g.Населенный_пункт,
            g.Внутригородская_тер,
            g.Город,
            g.Район,
            g.Автономный_окр,
            g.Субъект,
			g.СНТ,
			g.Улица,
            'Не определено'
        ) AS combinade_Geo_Gar,

        COALESCE(
            g2.Населенный_пункт,
            g2.Внутригородская_тер,
            g2.Город,
            g2.Район,
            g2.Автономный_окр,
            g2.Субъект,
			g2.СНТ,
			g2.Улица,
            'Не определено'
        ) AS combinade_Geo_Gar_Sensus

	into #geo_data

    FROM [SSA].[dbo].[hs_MDLP_reestr_partners] rtl_hash
    LEFT JOIN [SSA].[dbo].[Apt] a
        ON rtl_hash.[hs_apt] = a.[hs_auto_gen]
    LEFT JOIN [SSA].[dbo].[Сенсус клиентов] sen
        ON a.[ID РНС] = sen.[ID РНС]
    LEFT JOIN #gar g
        ON rtl_hash.branch_address_fias_houseguid = g.objectguid
    LEFT JOIN #gar g2
        ON sen.[Код ФИАС] = g2.objectguid
    WHERE rtl_hash.branch_address_resolved_address <> 'Не найдена действующая лицензия по данному адресу'
      AND a.[ID РНС] < '1000000000'

-- Формируем справочник с ID RNC, где по коду ФИАС МДЛП получилось "не определено", но есть наш код ФИАС<>не определено
Select distinct [ID РНС],[код фиас] as branch_address_fias_houseguid
	into #MDLP_isnull
	From #geo_data
	Where combinade_Geo_Gar='Не определено' and combinade_Geo_Gar_Sensus<>'не определено'

-- Фильтруеи ID РНС, у которых только 1 уникальный код фиас
SELECT [ID РНС]
	into #one_fias_rns	
    FROM #geo_data
    GROUP BY [ID РНС]
    HAVING COUNT(DISTINCT branch_address_fias_houseguid) = 1

-- Финальный отбор строк, у которых для ID RNC есть только 1 код фиас
SELECT distinct [ID РНС],branch_address_fias_houseguid
into #Final_1_code_Fias
FROM #geo_data
WHERE combinade_Geo_Gar = combinade_Geo_Gar_Sensus
  AND [ID РНС] IN (SELECT [ID РНС] FROM #one_fias_rns) and combinade_Geo_Gar<>'не определено'

-- Дом для айди рнс = нулл либо в Фиас_Дом, либо в ФиасСен_Дом (Чтобы исключить эти строки в дальнейшем)
Select distinct [ID РНС]
into #null_house
	From #geo_data
	Where ФиасСен_Дом is null or Фиас_Дом is null

-- Группируем строки, чтобы увидеть, в каких случаях география на айди одинаковая не смотря на наличие нескольких кодов ФИАС
SELECT distinct Фиас_lev5, Фиас_lv6, Фиас_lv4, Фиас_lv3, Фиас_Lv1, Фиас_Lv8, Фиас_Дом, ФиасСен_lev5, ФиасСен_lv6, ФиасСен_lv4, ФиасСен_lv3, ФиасСен_Lv2, ФиасСен_Lv1, ФиасСен_Lv7, ФиасСен_Lv8, ФиасСен_Дом, [ID РНС],
COUNT(*) OVER (PARTITION BY [ID РНС]) AS Счет_Если_по_ID
into #distinctID
FROM #geo_data
WHERE 
   [ID РНС] Not IN (SELECT [ID РНС] FROM #one_fias_rns) and [ID РНС] Not IN (select [ID РНС] from #null_house)
    GROUP BY 
        Фиас_lev5, Фиас_lv6, Фиас_lv4, Фиас_lv3, Фиас_Lv1, Фиас_Lv8, Фиас_Дом,
        ФиасСен_lev5, ФиасСен_lv6, ФиасСен_lv4, ФиасСен_lv3, ФиасСен_Lv2,
        ФиасСен_Lv1, ФиасСен_Lv7, ФиасСен_Lv8, ФиасСен_Дом,
        [ID РНС]

-- Формируем результирующую таблицу по айди рнс с несколькими кодами фиас
SELECT DISTINCT 
    g.[ID РНС],
    fias_grouped.fias_list AS branch_address_fias_houseguid
INTO #Final_code_Fias
FROM #geo_data g
JOIN #distinctID d ON g.[ID РНС] = d.[ID РНС]
JOIN (
    SELECT 
        [ID РНС],
        STRING_AGG(branch_address_fias_houseguid, ', ') 
            WITHIN GROUP (ORDER BY branch_address_fias_houseguid) AS fias_list
    FROM (
        SELECT DISTINCT [ID РНС], branch_address_fias_houseguid
        FROM #geo_data
    ) AS deduped
    GROUP BY [ID РНС]
) fias_grouped ON g.[ID РНС] = fias_grouped.[ID РНС]
WHERE 
    d.Фиас_Дом = d.ФиасСен_Дом 
    AND d.Счет_Если_по_ID = 1;


Select *
From #MDLP_isnull
union all
Select *
From #Final_code_Fias
union all
Select*
From #Final_1_code_Fias
