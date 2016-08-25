package magina

// fail fast if critical error happened.
func failOnError(err error) {
	if err != nil {
		panic(err)
	}
}
