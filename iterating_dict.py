for k in numbers:
    print("{} = {}".format(k, numbers[k]))

    
for planet, initial in planet_to_initial.items():
    print("{} begins with \"{}\"".format(planet.rjust(10), initial))    
    
 def word_search(documents, keyword):
    indices = []
    for i, doc in enumerate(documents):
        tokens = doc.split()
        normalized = [token.rstrip('.,').lower() for token in tokens]
        if keyword.lower() in normalized:
            indices.append(i)
    return indices   
