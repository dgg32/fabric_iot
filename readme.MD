
# Introduction

  

This repository contains code and data for my article "[From Sensor to Dashboard: Analyzing SwitchBot IoT Data with Microsoft Fabric](https://medium.com/@dgg32/from-sensor-to-dashboard-analyzing-switchbot-iot-data-with-microsoft-fabric-3a791692fea6)".



# Prerequisite
Fabric account

SwitchBot Hub


# Run

1. Set up the hardware and Fabric environment as described in the article.

2. Create a config.yaml file with the following structure:
```yaml
switchbot_token: "Your SwitchBot token here"
switchbot_secret: "Your SwitchBot secret here"
eventhub_connection_string: "Your Event Hub connection string here"
```

3. Run the switchbot_send_to_eventhub.py to send data to Event Hub.
```
python switchbot_send_to_eventhub.py
```


## Authors



*  **Sixing Huang** - *Concept and Coding*

  

  

## License

  

  

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details
