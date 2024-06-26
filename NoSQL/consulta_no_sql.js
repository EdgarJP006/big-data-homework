db.runCommand({
  mapReduce: 'restaurants',
  query: { 'grades.score': { $gte: 12 },'borough': 'Manhattan', 'cuisine': { $in: ['Spanish', 'Italian'] } },
  map: function Map() {
      var key = this.address.street;
      emit(key, {
          'data': [{
              'name': this.name,
              'x': this.address.coord[0],
              'y': this.address.coord[1],
              'address': this.address,
              'cuisine': this.cuisine
          }]
      });
  },
  reduce: function ReduceCode(key, values) {
      var reduced = { 'data': [] };
      for (var i in values) {
          var inter = values[i];
          for (var j in inter.data) {
              reduced.data.push(inter.data[j]);
          }
      }
      return reduced;
  },
  finalize: function Finalize(key, reduced) {
      if (reduced.data.length == 1) {
          return null; // Si hay solo un restaurante en la calle, no emitir resultados
      }
      var min_dist = Infinity;
      var bestRests = [];
      for (var i in reduced.data) {
          for (var j in reduced.data) {
              if (i >= j) continue;
              var r1 = reduced.data[i];
              var r2 = reduced.data[j];
              var distance = Math.sqrt(Math.pow(r1.x - r2.x, 2) + Math.pow(r1.y - r2.y, 2));
              if (distance < min_dist) {
                  bestRests = [{
                      'restaurant1': r1.name,'cuisine1': r1.cuisine, 'address1': r1.address,
                      'restaurant2': r2.name, 'cuisine2': r2.cuisine,'address2': r2.address,
                      'distance': distance,
                      'cantidad_restaurantes_evaluados': reduced.data.length
                  }];
                  min_dist = distance;
              } else if (distance == min_dist) {
                  bestRests.push({
                      'restaurant1': r1.name,'cuisine1': r1.cuisine, 'address1': r1.address,
                      'restaurant2': r2.name, 'cuisine2': r2.cuisine, 'address2': r2.address,
                      'distance': distance,
                      'cantidad_restaurantes_evaluados': reduced.data.length
                  });
              }
          }
      }
      return bestRests;
  },
  out: { replace: "rest_mapreduce"}
});
