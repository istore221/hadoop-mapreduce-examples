from mrjob.job import MRJob
from mrjob.step import MRStep


# userid | movieid | rating | timestamp
# rating distribution
#python mr.py -r hadoop --hadoop-streaming-jar /opt/hadoop-2.9.0/share/hadoop/tools/lib/hadoop-streaming-2.9.0.jar u.data

class MRMovieRatingCount(MRJob):

    def steps(self):
        return [
             MRStep(mapper=self.my_mapper,
                   reducer=self.my_reducer)
        ]

    def my_mapper(self, _, line):
        userID, movieID, rating, timestamp = line.split('\t');
        yield rating,1

    def my_reducer(self, key, values):
        yield key,sum(values)


if __name__ == '__main__':
    MRMovieRatingCount.run()
