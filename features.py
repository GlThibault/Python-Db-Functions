import db

def predict_motifs (features):
    ids=[]
    motifs = []
    for feature in features:
        print (feature['id'])

products = db.get_products()
product_ids = [p['product_id'] for p in products]
offset = 0
features = db.get_features_offset(product_id=product_ids, offset=offset)

predict_motifs(features)
"""
offset = 0
while len(products) >= 100 :
    predict_motifs(features)
    offset += 100
    features = db.get_features(product_id=product_ids, offset=offset)
"""
