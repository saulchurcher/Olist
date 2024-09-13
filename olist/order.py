import pandas as pd
import numpy as np
from olist.utils import haversine_distance
from olist.data import Olist
olist = Olist()
data = olist.get_data()

class Order:
    '''
    DataFrames containing all orders as index,
    and various properties of these orders as columns
    '''
    def __init__(self):
        # Assign an attribute ".data" to all new instances of Order
        self.data = Olist().get_data()

    def get_wait_time(self, is_delivered=True):
        """
        Returns a DataFrame with:
        [order_id, wait_time, expected_wait_time, delay_vs_expected, order_status]
        and filters out non-delivered orders unless specified
        """
        # Hint: Within this instance method, you have access to the instance of the class Order in the variable self, as well as all its attributes
        olist = Olist()
        data = olist.get_data()

        orders = data['orders'].copy()
        assert(orders.shape == (99441, 8))

        orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'])
        orders['order_delivered_customer_date'] = pd.to_datetime(orders['order_delivered_customer_date'])
        orders['order_estimated_delivery_date'] = pd.to_datetime(orders['order_estimated_delivery_date'])

        orders['wait_time'] = pd.to_timedelta(orders['order_delivered_customer_date'] - orders['order_purchase_timestamp']) / pd.to_timedelta(1, unit='D')
        orders['expected_wait_time'] = pd.to_timedelta(orders['order_estimated_delivery_date'] - orders['order_purchase_timestamp']) / pd.to_timedelta(1, unit='D')
        orders['delay_vs_expected'] = pd.to_timedelta(orders['order_delivered_customer_date'] - orders['order_estimated_delivery_date']) / pd.to_timedelta(1, unit='D')
        orders.delay_vs_expected = orders.delay_vs_expected.map(lambda x : 0 if x < 0 else x)

        get_wait_time = orders[['order_id', 'wait_time', 'expected_wait_time', 'delay_vs_expected', 'order_status']]

        get_wait_time = get_wait_time.loc[get_wait_time['order_status'] == 'delivered']

        return get_wait_time


    def get_review_score(self):
        """
        Returns a DataFrame with:
        order_id, dim_is_five_star, dim_is_one_star, review_score
        """
        reviews = data['order_reviews'].copy()
        assert(reviews.shape == (99224,7))

        reviews['dim_is_five_star'] = reviews.review_score.map(lambda x : 1 if x == 5 else 0)
        reviews['dim_is_one_star'] = reviews.review_score.map(lambda x : 1 if x == 1 else 0)
        get_review_score = reviews[['order_id', 'dim_is_five_star', 'dim_is_one_star', 'review_score']]

        return get_review_score

    def get_number_items(self):
        """
        Returns a DataFrame with:
        order_id, number_of_items
        """
        order_items = data['order_items'].copy()

        get_number_items =  order_items.groupby('order_id').count().reset_index()[['order_id', 'order_item_id']].rename(columns= {'order_item_id' : 'number_of_items'})

        return get_number_items

    def get_number_sellers(self):
        """
        Returns a DataFrame with:
        order_id, number_of_sellers
        """
        order_items = data['order_items'].copy()

        get_number_sellers = order_items.groupby('order_id').count().reset_index()[['order_id' , 'seller_id']].rename(columns = {'seller_id' : 'number_of_sellers'})

        return get_number_sellers

    def get_price_and_freight(self):
        """
        Returns a DataFrame with:
        order_id, price, freight_value
        """
        order_items = data['order_items'].copy()

        get_price_and_freight = order_items.groupby('order_id').sum().reset_index()[['order_id' , 'price', 'freight_value']]

        return get_price_and_freight

    # Optional
    def get_distance_seller_customer(self):
        """
        Returns a DataFrame with:
        order_id, distance_seller_customer
        """
        # import data
        data = self.data
        orders = data['orders']
        order_items = data['order_items']
        sellers = data['sellers']
        customers = data['customers']

        # Since one zip code can map to multiple (lat, lng), take the first one
        geo = data['geolocation']
        geo = geo.groupby('geolocation_zip_code_prefix',
                          as_index=False).first()

        # Merge geo_location for sellers
        sellers_mask_columns = [
            'seller_id', 'seller_zip_code_prefix', 'geolocation_lat', 'geolocation_lng'
        ]

        sellers_geo = sellers.merge(
            geo,
            how='left',
            left_on='seller_zip_code_prefix',
            right_on='geolocation_zip_code_prefix')[sellers_mask_columns]

        # Merge geo_location for customers
        customers_mask_columns = ['customer_id', 'customer_zip_code_prefix', 'geolocation_lat', 'geolocation_lng']

        customers_geo = customers.merge(
            geo,
            how='left',
            left_on='customer_zip_code_prefix',
            right_on='geolocation_zip_code_prefix')[customers_mask_columns]

        # Match customers with sellers in one table
        customers_sellers = customers.merge(orders, on='customer_id')\
            .merge(order_items, on='order_id')\
            .merge(sellers, on='seller_id')\
            [['order_id', 'customer_id','customer_zip_code_prefix', 'seller_id', 'seller_zip_code_prefix']]

        # Add the geoloc
        matching_geo = customers_sellers.merge(sellers_geo,
                                            on='seller_id')\
            .merge(customers_geo,
                   on='customer_id',
                   suffixes=('_seller',
                             '_customer'))
        # Remove na()
        matching_geo = matching_geo.dropna()

        matching_geo.loc[:, 'distance_seller_customer'] =\
            matching_geo.apply(lambda row:
                               haversine_distance(row['geolocation_lng_seller'],
                                                  row['geolocation_lat_seller'],
                                                  row['geolocation_lng_customer'],
                                                  row['geolocation_lat_customer']),
                               axis=1)
        # Since an order can have multiple sellers,
        # return the average of the distance per order
        order_distance =\
            matching_geo.groupby('order_id',
                                 as_index=False).agg({'distance_seller_customer':
                                                      'mean'})

        return order_distance
        # $CHALLENGIFY_END

    def get_training_data(self,
                          is_delivered=True,
                          with_distance_seller_customer=False):
        """
        Returns a clean DataFrame (without NaN), with the all following columns:
        ['order_id', 'wait_time', 'expected_wait_time', 'delay_vs_expected',
        'order_status', 'dim_is_five_star', 'dim_is_one_star', 'review_score',
        'number_of_items', 'number_of_sellers', 'price', 'freight_value',
        'distance_seller_customer']
        """
        # Hint: make sure to re-use your instance methods defined above

        get_training_data_1 = Order.get_wait_time(self).merge(Order.get_review_score(self), on='order_id')

        get_training_data_2 = get_training_data_1.merge(Order.get_number_items(self), on='order_id')

        get_training_data_3 = get_training_data_2.merge(Order.get_number_sellers(self), on='order_id')

        get_training_data_4 = get_training_data_3.merge(Order.get_price_and_freight(self), on='order_id')

        get_training_data = get_training_data_4.merge(Order.get_distance_seller_customer(self), on='order_id')

        return get_training_data.dropna()
