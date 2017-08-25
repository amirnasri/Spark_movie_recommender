import pandas as pd
import numpy as np
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import os
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from scipy.sparse import csr_matrix
from sklearn.preprocessing import normalize
import pickle
import sys
from pyspark.sql.functions import rand
import logging


def load_data(spark, dataset):
	if dataset == 'ml-100k':
		return load_data_ml_100k(spark)
	if dataset == 'ml-1m':
		return load_data_ml_1m(spark)
	if dataset == 'ml-10m':
		return load_data_ml_10m(spark)


def load_data_ml_10m(spark):
	data_folder = 'data/ml-10m/'
	# Load rating table
	ratings = pd.read_csv(data_folder + 'ratings.dat', delimiter='::', engine='python', header=None)
	ratings.columns = ['user_id', 'movie_id', 'rating', 'timestamp']
	del ratings['timestamp']

	# Load movie table
	movies = pd.read_csv(data_folder + 'movies.dat', delimiter='::', engine='python', header=None)

	# Movie table columns as provided in the ReadMe file
	columns = 'movie id | movie title | genres'.split('|')

	movies.columns = ['_'.join(i.strip().split()) for i in columns]

	movies = movies[['movie_id', 'movie_title']]

	mi = ratings['movie_id'].unique()
	mi.sort()
	# movie-ID: id's provided in the movie table
	# movie-index: index ranges from 0 to #(unique movies) - 1
	movie_index_to_ID = dict(zip(range(len(mi)), mi))
	movie_ID_to_index = {k: v for v, k in movie_index_to_ID.iteritems()}

	sc = spark.sparkContext
	movie_ID_to_index_bc = sc.broadcast(movie_ID_to_index)

	# TODO: Should use broadcast for movie_ID_to_index?
	def parse_user_data(line):
		fields = line.split('::')
		user_id = int(fields[0])
		movie_id = int(fields[1])
		rating = float(fields[2])
		return movie_ID_to_index_bc.value[movie_id], user_id - 1, rating

	ratings_rdd = sc.textFile(data_folder + 'ratings.dat').map(parse_user_data)  # .cache().filter(lambda x: x is not None)

	ratings_columns = ['user_id', 'movie_id', 'rating']
	ratings_sp = spark.createDataFrame(ratings_rdd, schema=ratings_columns)
	return (movies, ratings, ratings_sp,
	        movie_index_to_ID, movie_ID_to_index)

def load_data_ml_1m(spark):
	data_folder = 'data/ml-1m/'
	# Load rating table
	ratings = pd.read_csv(data_folder + 'ratings.dat', delimiter='::', engine='python', header=None)
	ratings.columns = ['user_id', 'movie_id', 'rating', 'timestamp']
	del ratings['timestamp']

	# Load movie table
	movies = pd.read_csv(data_folder + 'movies.dat', delimiter='::', engine='python', header=None)

	# Movie table columns as provided in the ReadMe file
	columns = 'movie id | movie title | genres'.split('|')

	movies.columns = ['_'.join(i.strip().split()) for i in columns]

	movies = movies[['movie_id', 'movie_title']]

	mi = ratings['movie_id'].unique()
	mi.sort()
	# movie-ID: id's provided in the movie table
	# movie-index: index ranges from 0 to #(unique movies) - 1
	movie_index_to_ID = dict(zip(range(len(mi)), mi))
	movie_ID_to_index = {k: v for v, k in movie_index_to_ID.iteritems()}

	sc = spark.sparkContext
	movie_ID_to_index_bc = sc.broadcast(movie_ID_to_index)

	# TODO: Should use broadcast for movie_ID_to_index?
	def parse_user_data(line):
		fields = line.split('::')
		user_id = int(fields[0])
		movie_id = int(fields[1])
		rating = float(fields[2])
		return movie_ID_to_index_bc.value[movie_id], user_id - 1, rating

	ratings_rdd = sc.textFile(data_folder + 'ratings.dat').map(parse_user_data)  # .cache().filter(lambda x: x is not None)

	ratings_columns = ['user_id', 'movie_id', 'rating']
	ratings_sp = spark.createDataFrame(ratings_rdd, schema=ratings_columns)
	return (movies, ratings, ratings_sp,
	        movie_index_to_ID, movie_ID_to_index)


def load_data_ml_100k(spark):
	data_folder = 'data/ml-100k/'
	# Load rating table
	ratings = pd.read_csv(data_folder + 'u.data', delimiter='\t', engine='python', header=None)
	ratings.columns = ['user_id', 'movie_id', 'rating', 'timestamp']
	del ratings['timestamp']

	# Load movie table
	movies = pd.read_csv(data_folder + 'u.item', delimiter='|', engine='python', header=None)

	# Movie table columns as provided in the ReadMe file
	columns = 'movie id | movie title | release date | video release date |' \
	          'IMDB url | unknown | Action | Adventure | Animation |' \
	          'Children | Comedy | Crime | Documentary | Drama | Fantasy |' \
	          'Film-Noir | Horror | Musical | Mystery | Romance | Sci-Fi |' \
	          'Thriller | War | Western'.split('|')

	movies.columns = ['_'.join(i.strip().split()) for i in columns]

	movies = movies[['movie_id', 'movie_title', 'IMDB_url']]

	mi = ratings['movie_id'].unique()
	mi.sort()
	# movie-ID: id's provided in the movie table
	# movie-index: index ranges from 0 to #(unique movies) - 1
	movie_index_to_ID = dict(zip(range(len(mi)), mi))
	movie_ID_to_index = {k: v for v, k in movie_index_to_ID.iteritems()}

	sc = spark.sparkContext
	movie_ID_to_index_bc = sc.broadcast(movie_ID_to_index)

	# TODO: Should use broadcast for movie_ID_to_index?
	def parse_user_data(line):
		fields = line.split('\t')
		user_id = int(fields[0])
		movie_id = int(fields[1])
		rating = float(fields[2])
		return movie_ID_to_index_bc.value[movie_id], user_id - 1, rating

	ratings_rdd = sc.textFile(data_folder + 'u.data').map(parse_user_data)  # .cache().filter(lambda x: x is not None)

	ratings_columns = ['user_id', 'movie_id', 'rating']
	ratings_sp = spark.createDataFrame(ratings_rdd, schema=ratings_columns)
	return (movies, ratings, ratings_sp,
	        movie_index_to_ID, movie_ID_to_index)


def data_analysis(movies, ratings):
	print 'The following movie id\'s are missing from movie table'
	print sorted(set(range(1, movies.movie_id.max())) - set(movies.movie_id))
	print '\nnumber of unique movies: %s\n' %  len(set(movies.movie_id))

	# movie id have some missing values in addition to the missing values above
	# , i.e., there are movies that are not rated by any user.
	mi = ratings['movie_id'].unique()
	mi.sort()
	print 'The following movie id\'s exist in movie table are not rated by any user'
	print sorted(set(movies.movie_id) - set(mi))
	print len(mi)

	# movie-ID: id's provided in the movie table
	# movie-index: index ranges from 0 to #(unique movies) - 1
	movie_index_to_ID = dict(zip(range(len(mi)), mi))
	movie_ID_to_index = {k: v for v, k in movie_index_to_ID.iteritems()}

	return movie_index_to_ID, movie_ID_to_index


def row_style_to_coordinate(m):
	"""	Change from row-style to coordinate style matrix:
	m = [
	(0, (v00, v01, v02))
	(2, (v20, v21, v22))
	(5, (v50, v51, v52))
	]

	=>

	[
	(0, [(0, v00), (1, v01), (2, v02)])
	(2, [(0, v20), (1, v21), (2, v22)])
	(5, [(0, v50), (1, v51), (2, v52)])
	]

	=>

	[
	(0, 0, v00), (0, 1, v01), (0, 2, v02),
	(2, 0, v20), (2, 1, v21), (2, 2, v22),

	]
	"""
	x = m.map(lambda r: (r[0], zip(range(len(r[1])), r[1])))
	return x.flatMap(lambda r: [(r[0], i[0], i[1]) for i in r[1]])

def coordinate_to_sparse(m):
    row, col, data = np.array(m).T
    return csr_matrix((data, (row, col)))

def save_obj(obj, name ):
    with open(name + '.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

def param_cv(ratings_sp):

	def kfold_cv_generator(dataset, n_folds=3, seed=None):
		if not seed:
			seed = 0
		h = 1.0 / n_folds
		randCol = "cv_rand"
		df = dataset.select("*", rand(seed).alias(randCol)).cache()
		for i in range(n_folds):
			validateLB = i * h
			validateUB = (i + 1) * h
			condition = (df[randCol] >= validateLB) & (df[randCol] < validateUB)
			validation = df.filter(condition)
			train = df.filter(~condition)
			yield train.drop(randCol), validation.drop(randCol)

	param_map = {'rank': [1, 2, 4, 10], 'lambda_': np.logspace(-3, 0, 5)}
	#param_map = {'rank': [2, 10], 'lambda_': [ 1.0]}

	value_grid = np.meshgrid(*param_map.values(), indexing='xy')
	print value_grid
	index_grid = np.meshgrid(*[range(len(i)) for i in param_map.values()], indexing='ij')
	index_grids = []
	for a in index_grid:
		index_grids.append(a.reshape(-1, ))

	value_grids = []
	for a in value_grid:
		value_grids.append(a.reshape(-1, ))

	# index_grids = np.vstack(index_grids).T
	# value_grids = np.vstack(value_grids).T
	index_grids = zip(*index_grids)
	value_grids = zip(*value_grids)

	def calculate_MSE(ratings_train, ratings_test, kwargs):
		model = ALS.train(ratings_train, iterations=10, **kwargs)

		test_data = ratings_test.rdd.map(lambda r: (r[0], r[1]))
		predictions = model.predictAll(test_data).map(lambda r: ((r[0], r[1]), r[2]))
		p = predictions.collect()
		ratesAndPreds = ratings_test.rdd.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
		return (ratesAndPreds.map(lambda r: (r[1][0] - r[1][1]) ** 2).mean()) ** .5


	n_folds = 3
	logging.info('n_folds = %s' % n_folds)
	MSE_grid = np.zeros(len(value_grids))
	for i, vg in enumerate(value_grids):
		kwargs = dict(zip(param_map.keys(), vg))
		MSE = np.zeros(n_folds)
		for j, (ratings_train, ratings_test) in enumerate(kfold_cv_generator(ratings_sp, n_folds)):
			print kwargs
			MSE[j] = calculate_MSE(ratings_train, ratings_test, kwargs)
			print("Mean Squared Error= \n%s" % MSE)
		MSE_grid[i] = np.mean(MSE)
		print(" MSE grid= " + str(MSE_grid))
		MSE_grid_reshape = MSE_grid.reshape(list(reversed([len(d) for d in param_map.values()])))
		print(" MSE grid_reshape= \n%s" % MSE_grid_reshape)

	best_params_index = np.argmin(MSE_grid)
	best_params = dict(zip(param_map.keys(), value_grids[best_params_index]))
	MSE_grid_ = np.reshape(MSE_grid, value_grid[0].shape)
	print best_params

	#plot_MSE()

	def plot_MSE():
		import matplotlib.pyplot as plt
		from matplotlib import cm

		fig = plt.figure()
		ax = fig.gca(projection='3d')

		# Make data.

		# Plot the surface.
		args = list(value_grid)
		args.append(MSE_grid_)
		surf = ax.plot_surface(*args, cmap=cm.coolwarm,
		                       linewidth=0, antialiased=False)
		ax.set_xlabel('$\lambda$')
		ax.set_ylabel('rank')
		# ax.set_xscale('log')

		# Add a color bar which maps values to colors.
		# fig.colorbar(surf, shrink=0.5, aspect=5)

		plt.show()


	best_MSE = MSE_grid[best_params_index]
	benchmark_MSE = calculate_MSE(ratings_train, ratings_test, {'rank': 1, 'lambda_': 1.0})

	MSE_gain = (benchmark_MSE - best_MSE)/benchmark_MSE * 100
	print(MSE_gain)
	logging.info('MSE_grid = %s' % MSE_grid)
	logging.info('MSE_grid_reshape = %s' % MSE_grid_reshape)
	logging.info('best_params = %s' % best_params)
	logging.info('MSE_benchmark = %f' % benchmark_MSE)
	logging.info('MSE_best = %f' % best_MSE)
	logging.info('MSE_gain = %2.2f%%' % MSE_gain)
	return best_params


def main():
	logging.basicConfig(filename='recommender.log', level=logging.INFO, filemode='w')

	# Setting up spark session and spark context
	spark = SparkSession \
	    .builder \
	    .appName('Recommender') \
	    .getOrCreate()

	sc = spark.sparkContext
	sc.setLogLevel('WARN')
	logger = sc._jvm.org.apache.log4j
	logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
	logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

	(movies, ratings, ratings_sp,
		movie_index_to_ID, movie_ID_to_index) = load_data(spark, 'ml-1m')
	print('ratings.shape=%s' % str(ratings.shape))

	data_analysis(movies, ratings)
	params = {'rank': 10}
	#params = param_cv(ratings_sp)
	model = ALS.train(ratings_sp, iterations=10, **params)

	pf_rdd = model.productFeatures()
	uf_rdd = model.userFeatures()
	user_features = row_style_to_coordinate(pf_rdd)
	product_features = row_style_to_coordinate(uf_rdd)
	#print coordinate_to_sparse(user_features.collect()).todense().shape
	#print coordinate_to_sparse(product_features.collect()).todense().shape

	pf_sparse = coordinate_to_sparse(product_features.collect())
	pf = np.array(pf_sparse.todense())

	pf_norm = normalize(pf, axis=1)

	pp_sim = np.dot(pf_norm, pf_norm.T)

	recom_movie_index = np.argsort(pp_sim[0, :])[::-1][:10]

	recom_movie_df = pd.merge(pd.DataFrame({'movie_id':[movie_index_to_ID[i] for i in recom_movie_index]}), movies,
	                          how='inner', on='movie_id', suffixes=('_x', '_y'))

	print recom_movie_df

	result_folder = './result'
	if len(sys.argv) > 1:
		result_folder = sys.argv[1]

	if not os.path.exists(result_folder):
		os.mkdir(result_folder)

	curr_dir = os.getcwd()
	os.chdir(result_folder)
	os.system('rm -f *')
	movies.to_csv('movies_df.csv')
	save_obj(movie_index_to_ID, 'movie_index_to_ID')
	save_obj(movie_ID_to_index, 'movie_ID_to_index')
	np.save('pp_sim', pp_sim)
	os.chdir(curr_dir)


if __name__ == '__main__':
	main()
