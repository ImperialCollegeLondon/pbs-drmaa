all: example wait

example : example.c
	gcc example.c -o example -ldrmaa -L/opt/drmaa/torque/lib  -I/opt/drmaa/torque/include
wait : wait.c
	gcc wait.c -o wait -ldrmaa -L/opt/drmaa/torque/lib -Wl,-R/opt/drmaa/torque/lib -I/opt/drmaa/torque/include
      
