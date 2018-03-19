from mrjob.job import MRJob
from mrjob.step import MRStep


# userid | movieid | rating | timestamp
# Populer Worst Movies 

class PopulerWorstMovies(MRJob):

    def steps(self):
        return [
             MRStep(mapper=self.my_mapper,
                   reducer=self.my_reducer),
             MRStep(reducer=self.my_finalreducer)
        ]

    def my_mapper(self, _, line):
        userID, movieID, rating, timestamp = line.split('\t');
        yield int(movieID),int(rating)

    def my_reducer(self, key, values):
        values = list(values);
        avg = sum(values) / len(values);
        if(avg <= 2):
            yield len(values),key
        else:
            pass

    def my_finalreducer(self, key, values):
            for val in values:
                yield val,key
        



if __name__ == '__main__':
    PopulerWorstMovies.run()
