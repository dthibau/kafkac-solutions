using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaProducer.Formation.model
{
    public class Position
    {
        // Propriété pour la longitude
        public double Longitude { get; set; }

        // Propriété pour la latitude
        public double Latitude { get; set; }

        // Constructeur sans paramètre pour faciliter la sérialisation/désérialisation
        public Position()
        {
        }

        // Constructeur avec paramètres pour initialiser l'objet plus facilement
        public Position(double longitude, double latitude)
        {
            Longitude = longitude;
            Latitude = latitude;
        }
    }
}
