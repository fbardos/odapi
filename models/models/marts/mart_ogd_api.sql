-- Convention: one mart for every indicator
select * from {{ ref('intm_ktzh_gp_bevoelkerung')}}
UNION ALL
select * from {{ ref('intm_ktzh_gp_auslaenderanteil')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_einwohner_staendig') }}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_versiegelte_flaeche_anteil')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_wanderungssaldo_p1000')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_int_wanderungssaldo_p1000')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_binnen_wanderungssaldo_p1000')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_geburtenueberschuss')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_bevoelkerungsdichte_gesamt')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_bevoelkerungsdichte_produktiv')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_altersstruktur_jugendquotient')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_altersstruktur_altersquotient')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_altersstruktur_greyingindex')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_auslaenderanteil_staendig')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_auslaenderanteil_deutsche')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_auslaenderanteil_franzoesisch')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_auslaenderanteil_europaeisch')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_geburten')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_heiraten')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_heiraten_anzahl')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_scheidungen')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_scheidungen_anzahl')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_todesfaelle')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_todesfaelle_anzahl')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_einbuergerungen')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_haushalt_1pax')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_haushalt_2pax')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_haushalt_3pax')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_haushalt_4pax')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_haushalt_5pluspax')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_haushaltsgroesse')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_siedlungsflaeche')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_gebaeudeareal_gesamt')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_industrieareal')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_verkehrsflaeche')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_landwirtschaftsflaeche')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_waldflaeche')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_unproduktivflaeche')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_kuenstlich_angelegt')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_vegetationslose_flaeche')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_wasserflaeche')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_arbeitsstaetten_sektor1')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_arbeitsstaetten_sektor2')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_arbeitsstaetten_sektor3')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_beschaeftigte_sektor1')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_beschaeftigte_sektor2')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_beschaeftigte_sektor3')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_neu_unternehmen')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_neu_arbeitsplaetze')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_bestand_unternehmen')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_bestand_arbeitsplaetze')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_schliessung_unternehmen')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_schliessung_arbeitsplaetze')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_bau_gebaeude')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_bau_gebaeude_anzahl')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_bau_wohnung')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_efh')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_zi34')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_lwz')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_bewohner_efh')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_geb_energiequelle')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_geb_heizoel')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_geb_gas')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_geb_waermepumpe')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_whg_energiequelle')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_whg_heizoel')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_whg_gas')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_whg_waermepumpe')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_elektroautos_anzahl')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_elektroautos_anteil')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_pendler_bilanz')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_pendler_saldo')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_sozialhilfe_anzahl')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_sozialhilfe_quote')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_museen')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_biblio')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_biblio_100k')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_kinos')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_kinosaele')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_kinosaele_3d')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_kinokomplexe')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_multiplexkino')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_kino_sitzplaetze')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_arbeitsstaetten_kultur')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_beschaeftigte_kultur')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_tax_reineinkommen_steuerpflichtig')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_tax_reineinkommen_einwohner')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_tax_reineinkommen_total')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_tax_steuerbar_steuerpflichtig')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_tax_steuerbar_einwohner')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_tax_steuerbar_total')}}
UNION ALL
select * from {{ ref('intm_bfs_statatlas_gymi_anteil')}}
UNION ALL
select * from {{ ref('intm_bfe_minergie')}}
UNION ALL
select * from {{ ref('intm_swisstopo_zweitwohnung')}}
UNION ALL
select * from {{ ref('intm_estv_db_jp')}}
UNION ALL
select * from {{ ref('intm_estv_db_np')}}
UNION ALL
select * from {{ ref('intm_stab_bev_demografisch_bilanz')}}
UNION ALL
select * from {{ ref('intm_stab_bev_zivilstand')}}
UNION ALL
select * from {{ ref('intm_stab_bev_heirat')}}
UNION ALL
select * from {{ ref('intm_stab_bev_scheidung')}}
UNION ALL
select * from {{ ref('intm_stab_bev_altersklasse')}}
UNION ALL
select * from {{ ref('intm_stab_bev_geburtsort')}}
UNION ALL
select * from {{ ref('intm_stab_bev_zuwegzug')}}
UNION ALL
select * from {{ ref('intm_stab_geb_bestand')}}
UNION ALL
select * from {{ ref('intm_stab_geb_flaeche')}}
UNION ALL
select * from {{ ref('intm_stab_geb_leerwhg')}}
UNION ALL
select * from {{ ref('intm_stab_bau_ausgaben')}}
UNION ALL
select * from {{ ref('intm_stab_raum_areal')}}
UNION ALL
select * from {{ ref('intm_stab_raum_noas')}}
UNION ALL
select * from {{ ref('intm_stab_arbeit_grenzgaenger')}}
