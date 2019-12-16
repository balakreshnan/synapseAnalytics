using System;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Devices.Client;
using Newtonsoft.Json;

namespace IoTSimulator
{
    /// <summary>
    /// A sensor represents a Smart Meter in the simulator.
    /// </summary>
    class Sensor
    {
        private DeviceClient _DeviceClient;
        private string _IotHubUri { get; set; }
        public string DeviceId { get; set; }
        public string DeviceKey { get; set; }
        public DeviceState State { get; set; }
        public string StatusWindow { get; set; }
        public string ReceivedMessage { get; set; }
        public double? ReceivedTemperatureSetting { get; set; }
        public double CurrentTemperature
        {
            get
            {
                double avgTemperature = 70;
                Random rand = new Random();
                double currentTemperature = avgTemperature + rand.Next(-6, 6);

                if (ReceivedTemperatureSetting.HasValue)
                {
                    // If we received a cloud-to-device message that sets the temperature, override with the received value.
                    currentTemperature = ReceivedTemperatureSetting.Value;
                }

                if (currentTemperature <= 68)
                    TemperatureIndicator = SensorState.Cold;
                else if (currentTemperature > 68 && currentTemperature < 72)
                    TemperatureIndicator = SensorState.Normal;
                else if (currentTemperature >= 72)
                    TemperatureIndicator = SensorState.Hot;

                return currentTemperature;
            }
        }
        public SensorState TemperatureIndicator { get; set; }

        public Sensor(string iotHubUri, string deviceId, string deviceKey)
        {
            _IotHubUri = iotHubUri;
            DeviceId = deviceId;
            DeviceKey = deviceKey;
            State = DeviceState.Registered;
        }
        public void InstallDevice(string statusWindow)
        {
            StatusWindow = statusWindow;
            State = DeviceState.Installed;
        }

        /// <summary>
        /// Connect a device to the IoT Hub by instantiating a DeviceClient for that Device by Id and Key.
        /// </summary>
        public void ConnectDevice()
        {
            //TODO: 17. Connect the Device to Iot Hub by creating an instance of DeviceClient
            //_DeviceClient = DeviceClient.Create(_IotHubUri, new DeviceAuthenticationWithRegistrySymmetricKey(..., ...));

            //Set the Device State to Ready
            State = DeviceState.Ready;
        }
        public void DisconnectDevice()
        {
            //Delete the local device client
            _DeviceClient = null;

            //Set the Device State to Activate
            State = DeviceState.Activated;
        }

        /// <summary>
        /// Send a message to the IoT Hub from the Smart Meter device
        /// </summary>
        public async void SendMessageAsync()
        {
            var telemetryDataPoint = new
            {
                id = DeviceId,
                time = DateTime.UtcNow.ToString("o"),
                temp = CurrentTemperature
            };

            //TODO: 18.Serialize the telemetryDataPoint to JSON
            //var messageString = JsonConvert...;

            //TODO: 19.Encode the JSON string to ASCII as bytes and create new Message with the bytes
            //var message = new Message(...);

            //TODO: 20.Send the message to the IoT Hub
            //var sendEventAsync = _DeviceClient?.SendEventAsync(...);
            //if (sendEventAsync != null) await...;
        }

        /// <summary>
        /// Check for new messages sent to this device through IoT Hub.
        /// </summary>
        public async void ReceiveMessageAsync()
        {
            try
            {
                // Receive a message from IoT Hub
                var receivedMessage = await _DeviceClient?.ReceiveAsync();
                if (receivedMessage == null)
                {
                    ReceivedMessage = null;
                    return;
                }

                //TODO: 21.Set the received message for this sensor to the string value of the message byte array
                //ReceivedMessage = Encoding.ASCII.GetString(...);
                if (double.TryParse(ReceivedMessage, out var requestedTemperature))
                {
                    ReceivedTemperatureSetting = requestedTemperature;
                }
                else
                {
                    ReceivedTemperatureSetting = null;
                }

                // Send acknowledgement to IoT Hub that the has been successfully processed.
                // The message can be safely removed from the device queue. If something happened
                // that prevented the device app from completing the processing of the message,
                // IoT Hub delivers it again.

                //TODO: 22.Send acknowledgement to IoT hub that the message was processed
                //await _DeviceClient?.CompleteAsync(...);
            }
            catch (NullReferenceException ex)
            {
                // The device client is null, likely due to it being disconnected since this method was called.
                System.Diagnostics.Debug.WriteLine("The DeviceClient is null. This is likely due to it being disconnected since the ReceiveMessageAsync message was called.");
            }
        }
    }

    public enum DeviceState
    {
        Registered,
        Installed,
        Activated,
        Ready,
        Transmit
    }
    public enum SensorState
    {
        Cold,
        Normal,
        Hot
    }
}
