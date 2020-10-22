//  Sweden coordinates:
const minLong = 10.5;
const maxLong = 24.9;
const minLat = 55.3;
const maxLat = 69.1;

export const getRandomInRange = (from, to, fixed) => {
    return (Math.random() * (to - from) + from).toFixed(fixed) * 1;
    // .toFixed() returns string, so ' * 1' is a trick to convert to number
}

export const createOneDataPoint = () => {
    const latitude = getRandomInRange(minLat, maxLat, 3)
    const longitude = getRandomInRange(minLong, maxLong, 3)

    // const latitude = getRandomInRange(-90, 90, 3)
    // const longitude = getRandomInRange(-180, 180, 3)
    // const temp_cels = getRandomInRange(-273.15, 50, 3)
    const temp_cels = getRandomInRange(-0, 50, 3)
    return {
        "temperatureKelvin": temp_cels,
        "coordinates": {
            "lat": latitude,
            "lon": longitude
        }
    }
}

const emptyData = new Array(2000).fill(undefined);
export const filledData = emptyData.map((element) => createOneDataPoint());