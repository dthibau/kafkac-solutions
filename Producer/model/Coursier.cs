using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace KafkaProducer.Formation.model
{
    public class Coursier
    {
        // Propriété pour la longitude
        public long Id { get; set; }

        // Propriété pour la latitude
        public Position Position { get; set; }

        // Constructeur sans paramètre pour faciliter la sérialisation/désérialisation
        public Coursier()
        {
        }

        // Constructeur avec paramètres pour initialiser l'objet plus facilement
        public Coursier(long id, Position position)
        {
            Id = id;
            Position = position;
        }

        public void move()
        {
            Random random = new Random();
            Position.Latitude += random.NextDouble() - 0.5;
            Position.Longitude += random.NextDouble() - 0.5;
        }
    }
}
