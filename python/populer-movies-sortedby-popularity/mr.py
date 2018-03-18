from mrjob.job import MRJob
from mrjob.step import MRStep


# userid | movieid | rating | timestamp
# rating distribution
#python mr.py -r hadoop --hadoop-streaming-jar /opt/hadoop-2.9.0/share/hadoop/tools/lib/hadoop-streaming-2.9.0.jar u.data


class MRPopulerMoviesSortedByPopularity(MRJob):

    def steps(self):
        return [
             MRStep(mapper=self.my_mapper,
                   reducer=self.my_reducer),
	     MRStep(reducer=self.sort_reducer)
        ]

    def my_mapper(self, _, line):
        userID, movieID, rating, timestamp = line.split('\t');
        yield movieID,1

    def my_reducer(self, key, values):
        yield str(sum(values)).zfill(5),key

    def sort_reducer(self, key, values):
            
            for val in values:
                yield val,key


if __name__ == '__main__':
    MRPopulerMoviesSortedByPopularity.run()
