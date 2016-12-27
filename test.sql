[
    {
        "$match": {
            "id": {
                "$lt": 11
            }
        }
    },{
        "$match": {
            "client.id": 2
        }
    }
]