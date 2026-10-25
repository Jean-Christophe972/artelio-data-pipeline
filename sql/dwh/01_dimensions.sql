-- Dimensions métier et client (upsert). Paramètre : %(depuis)s = début de la fenêtre incrémentale.

INSERT INTO dwh.dim_metier (metier_code, libelle)
SELECT code, libelle FROM app.metiers
ON CONFLICT (metier_code) DO UPDATE SET libelle = EXCLUDED.libelle;

-- Seuls les clients actifs dans la fenêtre sont recalculés ; identifiants pseudonymisés (HMAC)
INSERT INTO dwh.dim_client (client_pseudo, departement, segment, premiere_demande)
SELECT dwh.pseudo(c.id),
       (SELECT l.departement FROM app.logements l WHERE l.client_id = c.id ORDER BY l.cree_le LIMIT 1),
       CASE WHEN count(d.id) > 1 THEN 'recurrent' ELSE 'nouveau' END,
       min(d.cree_le)::date
FROM app.clients c
LEFT JOIN app.demandes d ON d.client_id = c.id
WHERE c.cree_le >= %(depuis)s OR c.derniere_activite >= %(depuis)s
   OR EXISTS (SELECT 1 FROM app.demandes d2 WHERE d2.client_id = c.id AND d2.cree_le >= %(depuis)s)
GROUP BY c.id
ON CONFLICT (client_pseudo) DO UPDATE
SET departement = EXCLUDED.departement, segment = EXCLUDED.segment, premiere_demande = EXCLUDED.premiere_demande;
