db.movies_metadata.aggregate([
    {
        $match: { genres: { $elemMatch: { name: "Comedy" } }, vote_count: { $gte: 50 } }
    },
    { $sort: { vote_average: -1, vote_count: -1, movieId: 1 } },
    { $project: { title: 1, vote_average: 1, vote_count: 1, movieId: 1, _id: 0 } }
]);