-- Faits "intervention" : nouvelles interventions et mises à jour (statut, fin, facture, avis).

INSERT INTO dwh.fait_intervention
SELECT dwh.pseudo(i.id),
       to_char(coalesce(i.fin_reelle, i.debut_prevu), 'YYYYMMDD')::integer,
       dm.metier_key, dz.zone_key, da.artisan_key, dc.client_key,
       f.montant_ttc, f.commission_artelio,
       round(extract(epoch FROM coalesce(i.debut_reel, i.debut_prevu) - d.cree_le) / 3600, 2),
       round(extract(epoch FROM i.fin_reelle - i.debut_reel) / 3600, 2),
       av.note
FROM app.interventions i
JOIN app.devis v ON v.id = i.devis_id
JOIN app.demandes d ON d.id = v.demande_id
JOIN dwh.dim_client dc ON dc.client_pseudo = dwh.pseudo(d.client_id)
JOIN dwh.dim_artisan da ON da.artisan_pseudo = dwh.pseudo(i.artisan_id) AND da.est_courant
LEFT JOIN app.logements l ON l.id = d.logement_id
LEFT JOIN dwh.dim_zone dz ON dz.departement = l.departement
LEFT JOIN dwh.dim_metier dm ON dm.metier_code = coalesce(d.metier_valide, d.metier_predit)
LEFT JOIN app.factures f ON f.intervention_id = i.id
LEFT JOIN app.avis av ON av.intervention_id = i.id
WHERE i.debut_prevu >= %(depuis)s OR i.fin_reelle >= %(depuis)s
ON CONFLICT (intervention_pseudo) DO UPDATE SET
    date_key = EXCLUDED.date_key,
    montant_ttc = EXCLUDED.montant_ttc,
    commission_artelio = EXCLUDED.commission_artelio,
    delai_intervention_h = EXCLUDED.delai_intervention_h,
    duree_h = EXCLUDED.duree_h,
    note_avis = EXCLUDED.note_avis;
