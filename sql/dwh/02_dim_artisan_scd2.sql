-- Dimension artisan historisée (SCD type 2) en deux temps, dans la même transaction :
--   1. on "ferme" la ligne courante des artisans dont un attribut a changé ;
--   2. on insère une nouvelle ligne courante pour les artisans nouveaux ou modifiés.
-- Les interventions passées restent rattachées aux attributs de l'artisan à l'époque.
-- (La source est recalculée dans chaque requête : les comptes n'ont pas le droit de créer
--  de table temporaire, par moindre privilège.)

WITH source AS (
    SELECT dwh.pseudo(a.id) AS artisan_pseudo,
           coalesce((SELECT min(m.metier_code) FROM app.artisan_metiers m WHERE m.artisan_id = a.id), 'inconnu') AS metier_principal,
           a.departements[1] AS departement_principal,
           CASE WHEN a.cree_le > now() - interval '6 months' THEN 'moins de 6 mois'
                WHEN a.cree_le > now() - interval '2 years' THEN '6 mois à 2 ans'
                ELSE 'plus de 2 ans' END AS tranche_anciennete,
           CASE WHEN a.note_moyenne IS NULL THEN 'sans note'
                WHEN a.note_moyenne >= 4.5 THEN '4,5 à 5'
                WHEN a.note_moyenne >= 4 THEN '4 à 4,5'
                ELSE 'moins de 4' END AS tranche_note
    FROM app.artisans a
)
UPDATE dwh.dim_artisan d
SET valide_au = now(), est_courant = false
FROM source s
WHERE d.artisan_pseudo = s.artisan_pseudo
  AND d.est_courant
  AND (d.metier_principal, d.departement_principal, d.tranche_anciennete, d.tranche_note)
      IS DISTINCT FROM (s.metier_principal, s.departement_principal, s.tranche_anciennete, s.tranche_note);

WITH source AS (
    SELECT dwh.pseudo(a.id) AS artisan_pseudo,
           coalesce((SELECT min(m.metier_code) FROM app.artisan_metiers m WHERE m.artisan_id = a.id), 'inconnu') AS metier_principal,
           a.departements[1] AS departement_principal,
           CASE WHEN a.cree_le > now() - interval '6 months' THEN 'moins de 6 mois'
                WHEN a.cree_le > now() - interval '2 years' THEN '6 mois à 2 ans'
                ELSE 'plus de 2 ans' END AS tranche_anciennete,
           CASE WHEN a.note_moyenne IS NULL THEN 'sans note'
                WHEN a.note_moyenne >= 4.5 THEN '4,5 à 5'
                WHEN a.note_moyenne >= 4 THEN '4 à 4,5'
                ELSE 'moins de 4' END AS tranche_note
    FROM app.artisans a
)
INSERT INTO dwh.dim_artisan (artisan_pseudo, metier_principal, departement_principal, tranche_anciennete, tranche_note, valide_du)
SELECT s.artisan_pseudo, s.metier_principal, s.departement_principal, s.tranche_anciennete, s.tranche_note, now()
FROM source s
WHERE NOT EXISTS (SELECT 1 FROM dwh.dim_artisan d WHERE d.artisan_pseudo = s.artisan_pseudo AND d.est_courant);
