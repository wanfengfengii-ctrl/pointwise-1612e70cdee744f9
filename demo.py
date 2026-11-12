import json
from warehouse import Store
from samples import INITIAL, CORRECTION

def main():
    store = Store()
    try:
        for batch in (INITIAL,CORRECTION):
            result = store.apply(batch)
            print(json.dumps({"receipt":result,"snapshot":store.read()},ensure_ascii=False,sort_keys=True))
    finally:
        store.close()

if __name__ == "__main__":
    main()
