# main.py
from scraping import scrape_data
#from transformacion import transform_data
#from almacenamiento import store_data
#from visualizacion import visualize_data

def main():
    data = scrape_data()
#    transformed_data = transform_data(data)
#    store_data(transformed_data)
#    visualize_data(transformed_data)

if __name__ == "__main__":
    main()