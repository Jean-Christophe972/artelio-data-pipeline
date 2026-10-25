-- Faits "demande" : insertion des nouvelles demandes et mise à jour de celles de la fenêtre
-- (une demande évolue : devis reçus, acceptation, correction humaine du métier).

INSERT INTO dwh.fait_demande
SELECT dwh.pseudo(d.id),
       to_char(d.cree_le, 'YYYYMMDD')::integer,
       dm.metier_key, dz.zone_key, dc.client_key,
       d.urgence_predite, d.canal, d.confiance_ia,
       d.metier_valide IS NOT NULL AND d.metier_valide IS DISTINCT FROM d.metier_predit,
       (SELECT count(*) FROM app.devis v WHERE v.demande_id = d.id),
       EXISTS (SELECT 1 FROM app.devis v WHERE v.demande_id = d.id AND v.statut = 'accepte'),
       (SELECT round(extract(epoch FROM min(v.cree_le) - d.cree_le) / 3600, 2) FROM app.devis v WHERE v.demande_id = d.id)
FROM app.demandes d
JOIN dwh.dim_client dc ON dc.client_pseudo = dwh.pseudo(d.client_id)
LEFT JOIN app.logements l ON l.id = d.logement_id
LEFT JOIN dwh.dim_zone dz ON dz.departement = l.departement
LEFT JOIN dwh.dim_metier dm ON dm.metier_code = coalesce(d.metier_valide, d.metier_predit)
WHERE d.cree_le >= %(depuis)s
ON CONFLICT (demande_pseudo) DO UPDATE SET
    metier_key = EXCLUDED.metier_key,
    zone_key = EXCLUDED.zone_key,
    urgence = EXCLUDED.urgence,
    confiance_ia = EXCLUDED.confiance_ia,
    correction_humaine = EXCLUDED.correction_humaine,
    nb_devis = EXCLUDED.nb_devis,
    convertie = EXCLUDED.convertie,
    delai_premier_devis_h = EXCLUDED.delai_premier_devis_h;
